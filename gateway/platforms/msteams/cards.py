"""Teams card builders + markdown-to-HTML converter (C5).

Extracted from :mod:`.adapter` so card construction has its own
test surface and can be stubbed independently of the Bot Framework
plumbing.  Three card shapes live here:

- **Adaptive Cards** — generic structured payloads for rich agent
  responses (polls, tables, action buttons).  Attached to outgoing
  activities via the standard ``application/vnd.microsoft.card.adaptive``
  contentType.
- **FileConsentCard** — the DM-only upload consent flow.  The bot
  proposes an upload with this card; Teams returns an ``invoke``
  activity carrying an upload URL when the user accepts.  The adapter
  then PUTs the bytes into that URL and posts a ``FileInfoCard`` so
  recipients see a normal file attachment.
- **file.download.info attachment** — channel / group uploads live in
  SharePoint, and Teams renders a file card when the activity carries
  this attachment referencing the SharePoint webUrl.

The markdown converter moved here from the adapter because it's a pure
text transform with no Bot Framework dependency, and both cards and
plain ``send()`` calls need to produce Teams-safe HTML.
"""

from __future__ import annotations

import html
import re
import uuid
from typing import Any, Dict, List, Optional

# Content types the Bot Framework recognises.
ADAPTIVE_CARD_CONTENT_TYPE = "application/vnd.microsoft.card.adaptive"
FILE_CONSENT_CONTENT_TYPE = "application/vnd.microsoft.teams.card.file.consent"
FILE_INFO_CONTENT_TYPE = "application/vnd.microsoft.teams.card.file.info"
FILE_DOWNLOAD_INFO_CONTENT_TYPE = (
    "application/vnd.microsoft.teams.file.download.info"
)


# ---------------------------------------------------------------------------
# Markdown → Teams HTML
# ---------------------------------------------------------------------------

_CODE_FENCE_RE = re.compile(r"```(\w+)?\n(.*?)```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`([^`\n]+)`")
_BOLD_RE = re.compile(r"\*\*([^*\n]+)\*\*")
_ITALIC_RE = re.compile(r"(?<!\*)\*([^*\n]+)\*(?!\*)")
_ITALIC_UNDER_RE = re.compile(r"(?<!_)_([^_\n]+)_(?!_)")
_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^\s\)]+)\)")
_LIST_ITEM_RE = re.compile(r"^(\s*)([-*]|\d+\.)\s+(.*)$")


def markdown_to_teams_html(text: str) -> str:
    """Convert a restricted markdown dialect to Teams-safe HTML.

    Output is suitable as the ``text`` field of an outgoing activity
    with ``textFormat="xml"``.  Input is HTML-escaped before markdown
    is applied, so stray ``<script>`` tokens from an LLM stay as text.
    """
    if not text:
        return ""

    placeholders: Dict[str, str] = {}

    def _stash(block_html: str) -> str:
        token = f"\x00CODE{len(placeholders)}\x00"
        placeholders[token] = block_html
        return token

    def _fence_sub(m: re.Match) -> str:
        body = html.escape(m.group(2).rstrip("\n"))
        return _stash(f"<pre><code>{body}</code></pre>")

    text = _CODE_FENCE_RE.sub(_fence_sub, text)

    def _inline_code_sub(m: re.Match) -> str:
        return _stash(f"<code>{html.escape(m.group(1))}</code>")

    text = _INLINE_CODE_RE.sub(_inline_code_sub, text)
    text = html.escape(text)
    text = _BOLD_RE.sub(r"<b>\1</b>", text)
    text = _ITALIC_RE.sub(r"<i>\1</i>", text)
    text = _ITALIC_UNDER_RE.sub(r"<i>\1</i>", text)
    text = _LINK_RE.sub(
        lambda m: f'<a href="{m.group(2)}">{m.group(1)}</a>', text,
    )

    lines = text.split("\n")
    rendered: List[str] = []
    list_buffer: List[str] = []
    list_kind: Optional[str] = None

    def _flush_list():
        nonlocal list_buffer, list_kind
        if list_buffer and list_kind:
            rendered.append(f"<{list_kind}>" + "".join(list_buffer) + f"</{list_kind}>")
            list_buffer = []
            list_kind = None

    for raw in lines:
        m = _LIST_ITEM_RE.match(raw)
        if m:
            kind = "ol" if m.group(2)[0].isdigit() else "ul"
            if list_kind and list_kind != kind:
                _flush_list()
            list_kind = kind
            list_buffer.append(f"<li>{m.group(3)}</li>")
        else:
            _flush_list()
            rendered.append(raw)

    _flush_list()
    text = "\n".join(rendered)

    for token, block in placeholders.items():
        text = text.replace(token, block)

    text = text.replace("\n\n", "<br><br>").replace("\n", "<br>")
    return text


# ---------------------------------------------------------------------------
# Adaptive Cards
# ---------------------------------------------------------------------------

def build_adaptive_card(
    body: List[Dict[str, Any]],
    actions: Optional[List[Dict[str, Any]]] = None,
    schema_version: str = "1.5",
) -> Dict[str, Any]:
    """Wrap an Adaptive Card body + actions as a Bot Framework attachment.

    ``body`` is the raw list of Adaptive Card elements (TextBlock, Input,
    Image, Container, etc.) the caller already knows how to construct.
    This helper stays schema-agnostic on purpose — the agent owns the
    card layout, we just transport it.
    """
    card: Dict[str, Any] = {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": schema_version,
        "body": body,
    }
    if actions:
        card["actions"] = actions
    return {
        "contentType": ADAPTIVE_CARD_CONTENT_TYPE,
        "content": card,
    }


def build_poll_card(
    question: str,
    options: List[str],
    *,
    is_multi_select: bool = False,
    submit_title: str = "Vote",
    submit_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build an Adaptive Card poll attachment.

    A poll is a thin convention on top of Adaptive Cards — a bold
    question ``TextBlock`` followed by an ``Input.ChoiceSet`` and a
    single ``Action.Submit``.  Teams returns the user's choice as an
    ``invoke`` activity with ``value = {"choice": "<value>", **submit_data}``
    (or a list when ``is_multi_select=True``); the adapter dispatches
    those through the same path as any other card-action invoke.

    Two design nits worth calling out:
    - choice values equal the visible titles.  If you need opaque
      identifiers (e.g. for a ballot-secrecy use case), build the
      Adaptive Card by hand with :func:`build_adaptive_card` instead.
    - ``style`` is always ``"expanded"`` so voters see every option
      without tapping the combobox — the typical UX for a one-shot
      poll.
    """
    if not options:
        raise ValueError("build_poll_card requires at least one option")

    choices = [{"title": opt, "value": opt} for opt in options]
    body: List[Dict[str, Any]] = [
        {
            "type": "TextBlock",
            "text": question,
            "weight": "Bolder",
            "wrap": True,
        },
        {
            "type": "Input.ChoiceSet",
            "id": "choice",
            "style": "expanded",
            "isMultiSelect": bool(is_multi_select),
            "choices": choices,
        },
    ]
    actions: List[Dict[str, Any]] = [
        {
            "type": "Action.Submit",
            "title": submit_title,
            "data": dict(submit_data or {}),
        },
    ]
    return build_adaptive_card(body=body, actions=actions)


# ---------------------------------------------------------------------------
# FileConsentCard — DM-only upload consent flow
# ---------------------------------------------------------------------------

def build_file_consent_card(
    filename: str,
    size_bytes: int,
    description: str = "",
    accept_context: Optional[Dict[str, Any]] = None,
    decline_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the attachment that kicks off a Teams FileConsent upload.

    Teams renders a "Bot wants to send you a file" card with Accept /
    Decline buttons.  On Accept, the Bot Framework posts an ``invoke``
    activity with ``name=fileConsent/invoke`` and an ``uploadInfo``
    object pointing at the OneDrive upload URL; the adapter PUTs the
    bytes there and replies with a :func:`build_file_info_card` so the
    file appears inline in the conversation.

    ``accept_context`` and ``decline_context`` are opaque dicts Teams
    echoes back on the invoke activity — typically a reference id the
    adapter uses to look up the pending upload.
    """
    context = dict(accept_context or {})
    # Every consent card carries a stable correlation id so the invoke
    # handler can tie the response back to the original upload request.
    context.setdefault("upload_id", str(uuid.uuid4()))
    return {
        "contentType": FILE_CONSENT_CONTENT_TYPE,
        "content": {
            "description": description or "File from Hermes",
            "sizeInBytes": int(size_bytes),
            "acceptContext": context,
            "declineContext": dict(decline_context or {}),
        },
        "name": filename,
    }


def build_file_info_card(
    filename: str,
    unique_id: str,
    file_type: str,
) -> Dict[str, Any]:
    """Post-upload card that makes the newly-uploaded file render as a
    first-class attachment in the DM."""
    return {
        "contentType": FILE_INFO_CONTENT_TYPE,
        "content": {
            "uniqueId": unique_id,
            "fileType": file_type,
        },
        "name": filename,
    }


# ---------------------------------------------------------------------------
# Channel / group uploads — file.download.info attachment
# ---------------------------------------------------------------------------

def build_file_download_card(
    filename: str,
    content_url: str,
    unique_id: Optional[str] = None,
    file_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Bot Framework attachment that renders a SharePoint-hosted file.

    ``content_url`` is the SharePoint ``webUrl`` returned by
    :meth:`GraphClient.upload_to_sharepoint`.  Teams clients fetch the
    preview + download link from that URL when rendering the message.
    """
    content: Dict[str, Any] = {
        "downloadUrl": content_url,
    }
    if unique_id:
        content["uniqueId"] = unique_id
    content["fileType"] = file_type or _infer_file_type(filename)
    return {
        "contentType": FILE_DOWNLOAD_INFO_CONTENT_TYPE,
        "contentUrl": content_url,
        "content": content,
        "name": filename,
    }


def _infer_file_type(filename: str) -> str:
    """Return the Teams-style file type token for *filename*.

    Teams accepts ``"png"``/``"jpg"``/``"pdf"``/... strings; the mapping
    lines up with common extension suffixes.  Files without a dotted
    extension (``LICENSE``, ``Makefile``) fall back to ``"file"`` so
    Teams renders a generic document icon.
    """
    _head, sep, ext = filename.rpartition(".")
    if not sep:
        return "file"
    return ext.lower() or "file"
