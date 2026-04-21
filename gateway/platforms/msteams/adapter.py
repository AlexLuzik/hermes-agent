"""Microsoft Teams Bot Framework adapter (C3 DM/channel/group protocol).

Receives activities over a dedicated aiohttp webhook (default port 3978,
path /api/messages), validates the Bot Framework JWT, parses the
:class:`botbuilder.schema.Activity` into a Hermes :class:`MessageEvent`,
applies DM / channel / group access policy, and dispatches into the
gateway via :py:meth:`BasePlatformAdapter.handle_message`.

Outbound replies go directly to the channel's reported ``serviceUrl``
with a Bearer token minted from the :mod:`.auth` credential provider.
Every incoming activity records its ``serviceUrl`` into
``~/.hermes/msteams/service_urls.json`` so the out-of-process
``_send_msteams`` helper in :mod:`tools.send_message_tool` can reach the
same conversation without the gateway running.

Richer features — attachment downloads, channel history, Adaptive
Cards, FileConsent uploads — arrive in C4 (Graph) and C5 (cards).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.platforms.msteams.auth import (
    BOT_FRAMEWORK_SCOPE,
    AuthError,
    CredentialProvider,
    build_credential_provider,
)
from gateway.platforms.msteams.cards import (
    build_adaptive_card,
    build_file_consent_card,
    build_file_download_card,
    build_file_info_card,
    markdown_to_teams_html,
)
from gateway.platforms.msteams.graph import GraphClient

# Re-exported for backward compatibility — the converter moved to
# cards.py in C5 but existing tests import it through this module.
__all__ = [
    "MsTeamsAdapter",
    "check_msteams_requirements",
    "markdown_to_teams_html",
    "strip_bot_mention",
    "build_adaptive_card",
    "build_file_consent_card",
    "build_file_download_card",
    "build_file_info_card",
]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dependency probe
# ---------------------------------------------------------------------------

def check_msteams_requirements() -> bool:
    """Return True iff every MS Teams runtime dependency imports cleanly.

    Gates the adapter factory in :mod:`gateway.run` so a gateway with
    Teams enabled but without the ``[msteams]`` extra installed logs a
    clear instruction instead of crashing.
    """
    try:
        import botbuilder.core  # noqa: F401
        import botbuilder.schema  # noqa: F401
        import botframework.connector  # noqa: F401
        import msal  # noqa: F401
        import azure.identity  # noqa: F401
        import msgraph  # noqa: F401
        import aiohttp  # noqa: F401
    except ImportError as exc:
        logger.debug("MSTeams dependency missing: %s", exc)
        return False
    return True


# ---------------------------------------------------------------------------
# Service-URL sidecar — reached by the out-of-process _send_msteams helper
# ---------------------------------------------------------------------------

def _service_urls_path():
    """Return the JSON sidecar path or ``None`` if HERMES_HOME is unusable.

    Imported lazily so the adapter module stays import-safe under
    unusual test environments that don't initialise HERMES_HOME.
    """
    try:
        from hermes_constants import get_hermes_home
        path = get_hermes_home() / "msteams" / "service_urls.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    except Exception:
        logger.debug("msteams: could not resolve service_urls.json path", exc_info=True)
        return None


def load_service_urls() -> Dict[str, str]:
    path = _service_urls_path()
    if path is None or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("msteams: service_urls.json is malformed; starting empty")
        return {}


def save_service_urls(mapping: Dict[str, str]) -> None:
    path = _service_urls_path()
    if path is None:
        return
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(mapping, indent=2), encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# @mention stripping
# ---------------------------------------------------------------------------

_MENTION_TAG_RE = re.compile(r"<at>[^<]*</at>\s?", re.IGNORECASE)


def strip_bot_mention(text: str, bot_id: str, bot_name: str) -> Tuple[str, bool]:
    """Remove the leading ``@botname`` mention from *text*.

    Returns ``(cleaned, was_mentioned)``.  Teams delivers channel posts
    with ``<at>BotName</at>`` HTML-ish markers inside ``text`` and a
    ``mentions`` array on the activity; we strip on both grounds.
    """
    if not text:
        return text, False

    was_mentioned = False

    # Strip <at>...</at> wrappers and detect whether any of them match
    # the bot's display name or ID.
    def _strip_at(m: re.Match) -> str:
        nonlocal was_mentioned
        inner = m.group(0).lower()
        if bot_name and bot_name.lower() in inner:
            was_mentioned = True
        if bot_id and bot_id.lower() in inner:
            was_mentioned = True
        return ""

    cleaned = _MENTION_TAG_RE.sub(_strip_at, text).strip()

    # Also catch raw "@BotName" prefixes for clients that render plain.
    if bot_name:
        prefix = f"@{bot_name.lower()}"
        if cleaned.lower().startswith(prefix):
            cleaned = cleaned[len(prefix):].lstrip(" ,:")
            was_mentioned = True

    return cleaned, was_mentioned


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

# Teams hard-caps an activity's ``text`` field at ~28k characters.
MAX_MESSAGE_LENGTH = 28000


class MsTeamsAdapter(BasePlatformAdapter):
    """Adapter that bridges Hermes' session router and the Bot Framework."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    REQUIRES_EDIT_FINALIZE = False

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.MSTEAMS)
        self.gateway_runner = None  # set by gateway.run._create_adapter
        extra: Dict[str, Any] = dict(config.extra or {})

        # Identity + auth strategy
        self._app_id: str = str(extra.get("app_id") or "").strip()
        self._app_password: str = str(extra.get("app_password") or "")
        self._tenant_id: str = str(extra.get("tenant_id") or "").strip()
        self._auth_type: str = str(extra.get("auth_type") or "secret").lower()
        self._bot_display_name: str = str(extra.get("bot_display_name") or "")

        # Webhook transport
        self._host: str = str(extra.get("host") or "0.0.0.0")
        self._port: int = int(extra.get("port") or 3978)
        self._path: str = str(extra.get("path") or "/api/messages")

        # Policy knobs (openclaw parity)
        self._require_mention: bool = bool(extra.get("require_mention", True))
        self._reply_style: str = str(extra.get("reply_style") or "thread")
        self._history_limit: int = int(extra.get("history_limit") or 50)
        self._dm_policy: str = str(extra.get("dm_policy") or "pairing")
        self._allow_from: List[str] = list(extra.get("allow_from") or [])
        self._group_allow_from: List[str] = list(extra.get("group_allow_from") or [])
        self._free_response_channels: List[str] = list(
            extra.get("free_response_channels") or []
        )

        # Per-team overrides (openclaw ``teams[<team_id>]`` block).
        # Each entry may override require_mention / reply_style / allow_from /
        # channels[<channel_id>] overrides.
        self._team_overrides: Dict[str, Dict[str, Any]] = dict(
            extra.get("teams") or {},
        )

        # SharePoint: required for channel/group file uploads via Graph.
        self._sharepoint_site_id: str = str(extra.get("sharepoint_site_id") or "")
        self._sharepoint_folder: str = str(extra.get("sharepoint_folder") or "Hermes")

        # Credentials + Graph are built lazily in connect() so a
        # misconfigured adapter can still be constructed for inspection
        # (status display, tests, setup wizards).
        self._extra_snapshot: Dict[str, Any] = extra
        self._credential_provider: Optional[CredentialProvider] = None
        self._graph: Optional[GraphClient] = None

        # Runtime state
        self._aiohttp_runner = None
        self._aiohttp_site = None
        self._service_urls: Dict[str, str] = load_service_urls()
        self._team_ids_by_chat: Dict[str, str] = {}
        self._http_session = None  # aiohttp.ClientSession, lazy
        self._save_lock = asyncio.Lock()
        # Pending DM uploads awaiting FileConsent response.  Keyed by
        # the upload_id stashed in the FileConsentCard acceptContext.
        # Bytes are buffered in memory — the consent/invoke response
        # completes quickly in practice so this doesn't grow.
        self._pending_uploads: Dict[str, Dict[str, Any]] = {}

    @property
    def name(self) -> str:
        return "msteams"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        if not self._app_id:
            self._set_fatal_error(
                "msteams_config",
                "MSTEAMS_APP_ID is required",
                retryable=False,
            )
            return False

        try:
            self._credential_provider = build_credential_provider(self._extra_snapshot)
        except AuthError as exc:
            self._set_fatal_error("msteams_auth", str(exc), retryable=False)
            return False

        self._graph = GraphClient(self._credential_provider)

        if not self._acquire_platform_lock(
            "msteams-endpoint",
            f"{self._host}:{self._port}",
            f"MS Teams endpoint {self._host}:{self._port}",
        ):
            return False

        try:
            from aiohttp import web
        except ImportError:
            self._set_fatal_error(
                "msteams_aiohttp", "aiohttp is required for MS Teams", retryable=False,
            )
            return False

        app = web.Application(client_max_size=4 * 1024 * 1024)
        app.router.add_post(self._path, self._handle_messages)
        app.router.add_get("/health", self._handle_health)

        self._aiohttp_runner = web.AppRunner(app)
        await self._aiohttp_runner.setup()
        self._aiohttp_site = web.TCPSite(self._aiohttp_runner, self._host, self._port)
        try:
            await self._aiohttp_site.start()
        except OSError as exc:
            self._set_fatal_error(
                "msteams_bind", f"Cannot bind {self._host}:{self._port}: {exc}",
                retryable=False,
            )
            await self._aiohttp_runner.cleanup()
            self._aiohttp_runner = None
            self._aiohttp_site = None
            return False

        self._mark_connected()
        logger.info(
            "msteams: listening on http://%s:%d%s (app_id=%s..., auth_type=%s)",
            self._host, self._port, self._path,
            self._app_id[:8], self._auth_type,
        )
        return True

    async def disconnect(self) -> None:
        if self._aiohttp_site is not None:
            with contextlib.suppress(Exception):
                await self._aiohttp_site.stop()
            self._aiohttp_site = None
        if self._aiohttp_runner is not None:
            with contextlib.suppress(Exception):
                await self._aiohttp_runner.cleanup()
            self._aiohttp_runner = None
        if self._http_session is not None:
            with contextlib.suppress(Exception):
                await self._http_session.close()
            self._http_session = None
        if self._graph is not None:
            with contextlib.suppress(Exception):
                await self._graph.close()
            self._graph = None
        if self._credential_provider is not None:
            with contextlib.suppress(Exception):
                await self._credential_provider.close()
            self._credential_provider = None
        self._release_platform_lock()
        self._mark_disconnected()

    # ------------------------------------------------------------------
    # HTTP handlers
    # ------------------------------------------------------------------

    async def _handle_health(self, request):
        from aiohttp import web
        return web.json_response({
            "platform": "msteams",
            "running": self._running,
            "app_id": self._app_id[:8] + "..." if self._app_id else None,
        })

    async def _handle_messages(self, request):
        from aiohttp import web

        try:
            raw = await request.read()
        except Exception as exc:
            logger.warning("msteams: failed to read request body: %s", exc)
            return web.Response(status=400, text="bad request")

        try:
            body = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            return web.Response(status=400, text="invalid JSON")

        auth_header = request.headers.get("Authorization", "")
        activity = await self._deserialize_activity(body)
        if activity is None:
            return web.Response(status=400, text="malformed activity")

        if not await self._validate_jwt(activity, auth_header):
            # ``_validate_jwt`` logs the specific failure.
            return web.Response(status=401, text="unauthorized")

        # Persist serviceUrl as early as possible so standalone senders
        # can reach this conversation even if subsequent logic drops the
        # message.
        service_url = getattr(activity, "service_url", None)
        conversation = getattr(activity, "conversation", None)
        chat_id = str(conversation.id) if conversation is not None else ""
        if chat_id and service_url:
            await self._remember_service_url(chat_id, str(service_url))

        activity_type = (getattr(activity, "type", "") or "").lower()

        if activity_type == "typing":
            return web.Response(status=200)
        if activity_type == "invoke":
            invoke_name = (getattr(activity, "name", "") or "").lower()
            if invoke_name == "fileconsent/invoke":
                status = await self._handle_file_consent_invoke(activity)
                return web.Response(status=status)
            return web.Response(status=200)
        if activity_type != "message":
            return web.Response(status=200)

        # Drop messages the bot sent to itself — prevents loops when two
        # gateways share an App ID.
        from_identity = getattr(activity, "from_property", None)
        from_id = str(getattr(from_identity, "id", "") or "")
        if from_id and self._app_id and from_id.endswith(self._app_id):
            return web.Response(status=200)

        try:
            event, dispatch = self._build_event(activity)
        except Exception:
            logger.exception("msteams: failed to build MessageEvent; dropping")
            return web.Response(status=200)

        if not dispatch or event is None:
            return web.Response(status=200)

        try:
            await self.handle_message(event)
        except Exception:
            logger.exception("msteams: handle_message raised")
        return web.Response(status=200)

    # ------------------------------------------------------------------
    # Activity parsing & policy
    # ------------------------------------------------------------------

    async def _deserialize_activity(self, body: Dict[str, Any]):
        try:
            from botbuilder.schema import Activity
        except ImportError:
            logger.error("msteams: botbuilder-schema not installed")
            return None
        try:
            return Activity().deserialize(body)
        except Exception:
            logger.warning("msteams: activity deserialize failed", exc_info=True)
            return None

    async def _validate_jwt(self, activity, auth_header: str) -> bool:
        """Validate the Bot Framework JWT on an incoming activity.

        Uses ``JwtTokenValidation.authenticate_request`` with a
        ``SimpleCredentialProvider``.  Federated-auth bots still validate
        here — inbound tokens are signed by Microsoft's public keys,
        independent of how we mint outbound tokens.  Empty ``app_id``
        disables auth entirely (local emulator testing only).
        """
        if not self._app_id:
            logger.warning(
                "msteams: app_id is empty, skipping JWT validation (emulator mode)",
            )
            return True
        try:
            from botframework.connector.auth import (
                JwtTokenValidation, SimpleCredentialProvider,
            )
        except ImportError:
            logger.error("msteams: botframework-connector not installed")
            return False
        creds = SimpleCredentialProvider(self._app_id, self._app_password)
        try:
            await JwtTokenValidation.authenticate_request(
                activity, auth_header, creds, channel_service_or_provider="",
            )
            return True
        except Exception as exc:
            logger.warning("msteams: JWT validation failed: %s", exc)
            return False

    def _build_event(self, activity) -> Tuple[Optional[MessageEvent], bool]:
        """Translate a Teams Activity into a ``(MessageEvent, dispatch)`` pair.

        Returns ``(None, False)`` when the message is silently dropped
        (policy denial, empty text).  Returns ``(event, True)`` when the
        gateway should dispatch.
        """
        conversation = getattr(activity, "conversation", None)
        if conversation is None:
            return None, False
        from_identity = getattr(activity, "from_property", None)
        if from_identity is None:
            return None, False

        chat_id = str(conversation.id or "")
        conversation_type = (
            getattr(conversation, "conversation_type", None) or "personal"
        ).lower()
        chat_type_map = {
            "personal": "dm",
            "groupchat": "group",
            "channel": "channel",
        }
        chat_type = chat_type_map.get(conversation_type, "dm")

        user_id = (
            getattr(from_identity, "aad_object_id", None)
            or getattr(from_identity, "id", None)
            or ""
        )
        user_id = str(user_id)
        user_name = str(getattr(from_identity, "name", "") or "") or None
        chat_name = str(getattr(conversation, "name", "") or "") or None

        # Teams channel info lives on activity.channel_data
        channel_data = getattr(activity, "channel_data", None) or {}
        team_id = None
        channel_id = None
        if isinstance(channel_data, dict):
            team = channel_data.get("team") or {}
            channel = channel_data.get("channel") or {}
            team_id = team.get("id")
            channel_id = channel.get("id")
            if team_id and chat_id:
                # Remember the parent team so get_chat_info / Graph
                # uploads can address the channel even after the
                # triggering activity is gone from adapter memory.
                self._team_ids_by_chat[chat_id] = team_id

        # The Bot Framework thread id for "threaded" conversations is the
        # conversation.id itself; replyToId points to the parent message.
        reply_to_id = str(getattr(activity, "reply_to_id", None) or "") or None

        raw_text = str(getattr(activity, "text", "") or "")
        cleaned_text, mentioned = strip_bot_mention(
            raw_text, self._app_id, self._bot_display_name,
        )

        # Resolve effective policy for this chat — per-team/channel
        # overrides layered onto the adapter defaults.
        effective = self._effective_policy(team_id=team_id, channel_id=channel_id)

        # Policy gate
        allowed, reason = self._policy_check(
            chat_type=chat_type,
            user_id=user_id,
            chat_id=chat_id,
            mentioned=mentioned,
            effective=effective,
        )
        if not allowed:
            logger.info(
                "msteams: dropping message (%s) from user=%s chat_type=%s",
                reason, user_id[:8] if user_id else "?", chat_type,
            )
            return None, False

        if not cleaned_text:
            return None, False

        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=user_id or None,
            user_name=user_name,
            thread_id=channel_id,  # channels partition sessions by channel
            chat_id_alt=team_id,   # remember parent team for later Graph calls
        )

        event = MessageEvent(
            text=cleaned_text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message={
                "service_url": str(getattr(activity, "service_url", "") or ""),
                "channel_data": channel_data if isinstance(channel_data, dict) else {},
                "activity_id": str(getattr(activity, "id", "") or ""),
            },
            message_id=str(getattr(activity, "id", "") or ""),
            reply_to_message_id=reply_to_id,
        )
        return event, True

    def _effective_policy(
        self, team_id: Optional[str], channel_id: Optional[str],
    ) -> Dict[str, Any]:
        """Layer per-team / per-channel overrides onto adapter defaults."""
        base = {
            "require_mention": self._require_mention,
            "reply_style": self._reply_style,
            "allow_from": list(self._allow_from),
            "group_allow_from": list(self._group_allow_from),
            "free_response_channels": list(self._free_response_channels),
        }
        if team_id and team_id in self._team_overrides:
            team_cfg = self._team_overrides[team_id]
            for key in ("require_mention", "reply_style"):
                if key in team_cfg:
                    base[key] = team_cfg[key]
            for list_key in ("allow_from", "group_allow_from", "free_response_channels"):
                if list_key in team_cfg:
                    base[list_key] = list(team_cfg[list_key])
            if channel_id:
                channels = team_cfg.get("channels") or {}
                channel_cfg = channels.get(channel_id)
                if channel_cfg:
                    for key in ("require_mention", "reply_style"):
                        if key in channel_cfg:
                            base[key] = channel_cfg[key]
                    for list_key in ("allow_from", "group_allow_from"):
                        if list_key in channel_cfg:
                            base[list_key] = list(channel_cfg[list_key])
        return base

    def _policy_check(
        self, *, chat_type: str, user_id: str, chat_id: str,
        mentioned: bool, effective: Dict[str, Any],
    ) -> Tuple[bool, str]:
        """Apply dm_policy / allowlist / requireMention gates.

        Returns ``(allowed, reason)``.  ``reason`` is present on deny to
        make log lines useful and empty on accept.
        """
        if chat_type == "dm":
            policy = (self._dm_policy or "pairing").lower()
            if policy == "disabled":
                return False, "dm_policy=disabled"
            if policy == "open":
                return True, ""
            if policy == "allowlist":
                if user_id in effective["allow_from"]:
                    return True, ""
                return False, "dm_allowlist"
            # "pairing" — hand off to Hermes pairing flow.  The base
            # adapter lets handle_message drive pairing; we accept here
            # and let the gateway decide.
            return True, ""

        # Channel or group
        if (
            effective["group_allow_from"]
            and user_id not in effective["group_allow_from"]
        ):
            return False, "group_allowlist"

        if effective["require_mention"] and chat_id not in effective["free_response_channels"]:
            if not mentioned:
                return False, "require_mention"

        return True, ""

    # ------------------------------------------------------------------
    # Service-URL persistence
    # ------------------------------------------------------------------

    async def _remember_service_url(self, chat_id: str, service_url: str) -> None:
        if self._service_urls.get(chat_id) == service_url:
            return
        self._service_urls[chat_id] = service_url
        async with self._save_lock:
            snapshot = dict(self._service_urls)
            await asyncio.to_thread(save_service_urls, snapshot)

    def _service_url_for(self, chat_id: str) -> Optional[str]:
        return self._service_urls.get(chat_id)

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    async def _get_http_session(self):
        import aiohttp
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._http_session

    def format_message(self, content: str) -> str:
        return markdown_to_teams_html(content)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        formatted = self.format_message(content)
        payload: Dict[str, Any] = {
            "type": "message",
            "textFormat": "xml",
            "text": formatted,
        }
        if reply_to:
            payload["replyToId"] = reply_to
        return await self._post_activity(chat_id, payload)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        await self._post_activity(chat_id, {"type": "typing"})

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata=None,
    ) -> SendResult:
        """Send an image.  In channels with SharePoint configured, downloads
        the image and re-uploads it so it renders as a first-class file
        card.  Everywhere else, falls back to emitting the URL in text.
        """
        if self._is_channel_chat(chat_id) and self._sharepoint_site_id:
            data, filename = await self._download_bytes(image_url)
            if data is not None:
                return await self._send_channel_file(
                    chat_id, filename or "image.png", data,
                    caption=caption, reply_to=reply_to,
                )
        parts: List[str] = []
        if caption:
            parts.append(caption)
        parts.append(image_url)
        return await self.send(chat_id, "\n".join(parts), reply_to=reply_to)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata=None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, image_path, caption, reply_to)

    async def send_document(
        self,
        chat_id: str,
        document_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata=None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, document_path, caption, reply_to)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata=None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, video_path, caption, reply_to)

    async def _send_local_file(
        self, chat_id: str, path: str,
        caption: Optional[str], reply_to: Optional[str],
    ) -> SendResult:
        """Dispatch a local file to the right Teams surface.

        DMs → FileConsentCard (user accepts; adapter PUTs bytes to the
        returned upload URL, then posts a FileInfoCard).  Channels and
        group chats → SharePoint upload via Graph + a file.download.info
        attachment so the file renders as a proper Teams file card.
        """
        import os
        if not path or not os.path.isfile(path):
            return SendResult(
                success=False, error=f"file not found: {path}", retryable=False,
            )
        try:
            with open(path, "rb") as fh:
                data = fh.read()
        except Exception as exc:
            return SendResult(
                success=False, error=f"read {path}: {exc}", retryable=False,
            )
        filename = os.path.basename(path)
        if self._is_channel_chat(chat_id):
            return await self._send_channel_file(
                chat_id, filename, data, caption=caption, reply_to=reply_to,
            )
        return await self._send_dm_file_consent(
            chat_id, filename, data, caption=caption, reply_to=reply_to,
        )

    def _is_channel_chat(self, chat_id: str) -> bool:
        # Teams channel conversation ids start with "19:".  Group and DM
        # conversations use different prefixes ("a:", "19:…@unq.gbl.spaces").
        # We treat any chat with a team id on record (or a "19:" prefix
        # without a DM suffix) as a channel for upload routing purposes.
        if chat_id in self._team_ids_by_chat:
            return True
        return chat_id.startswith("19:") and "@thread." in chat_id

    async def _send_channel_file(
        self, chat_id: str, filename: str, data: bytes,
        caption: Optional[str] = None, reply_to: Optional[str] = None,
    ) -> SendResult:
        """Upload *data* to SharePoint and post a file.download.info card."""
        if not self._sharepoint_site_id:
            return SendResult(
                success=False,
                error=(
                    "MSTEAMS_SHAREPOINT_SITE_ID is not configured — "
                    "channel file uploads require a SharePoint site"
                ),
                retryable=False,
            )
        url = await self.upload_channel_file(chat_id, filename, data)
        if not url:
            return SendResult(
                success=False,
                error="SharePoint upload failed (see msteams.graph log lines)",
                retryable=True,
            )
        attachment = build_file_download_card(filename, content_url=url)
        payload: Dict[str, Any] = {
            "type": "message",
            "attachments": [attachment],
        }
        if caption:
            payload["text"] = markdown_to_teams_html(caption)
            payload["textFormat"] = "xml"
        if reply_to:
            payload["replyToId"] = reply_to
        return await self._post_activity(chat_id, payload)

    async def _send_dm_file_consent(
        self, chat_id: str, filename: str, data: bytes,
        caption: Optional[str] = None, reply_to: Optional[str] = None,
    ) -> SendResult:
        """Initiate the FileConsent flow for a DM upload.

        The actual file bytes sit in ``_pending_uploads`` until the user
        accepts; the invoke handler completes the upload and posts a
        FileInfoCard.  A decline from the user simply drops the entry.
        """
        size = len(data)
        context = {"filename": filename, "service_url_chat_id": chat_id}
        card = build_file_consent_card(
            filename=filename, size_bytes=size,
            description=caption or f"Hermes wants to send you {filename}",
            accept_context=context,
            decline_context=context,
        )
        upload_id = card["content"]["acceptContext"]["upload_id"]
        self._pending_uploads[upload_id] = {
            "filename": filename,
            "bytes": data,
            "chat_id": chat_id,
            "caption": caption,
            "reply_to": reply_to,
        }
        payload: Dict[str, Any] = {
            "type": "message",
            "attachments": [card],
        }
        if caption:
            payload["text"] = markdown_to_teams_html(caption)
            payload["textFormat"] = "xml"
        if reply_to:
            payload["replyToId"] = reply_to
        return await self._post_activity(chat_id, payload)

    async def _handle_file_consent_invoke(self, activity) -> int:
        """Resolve a FileConsent accept/decline invoke.

        Returns the HTTP status the adapter should reply with.  Any
        recoverable error returns 200 (Teams stops retrying) after
        logging — silence is better than a retry loop for a declined
        upload.
        """
        value = getattr(activity, "value", None)
        if not isinstance(value, dict):
            logger.warning("msteams: fileConsent/invoke without value dict")
            return 200
        action = str(value.get("action") or "").lower()
        context = value.get("context") or {}
        upload_id = str(context.get("upload_id") or "")
        pending = self._pending_uploads.pop(upload_id, None) if upload_id else None
        if pending is None:
            logger.info(
                "msteams: fileConsent invoke for unknown upload_id=%r", upload_id,
            )
            return 200
        if action != "accept":
            logger.info(
                "msteams: fileConsent declined for %s", pending["filename"],
            )
            return 200

        upload_info = value.get("uploadInfo") or {}
        upload_url = upload_info.get("uploadUrl")
        unique_id = upload_info.get("uniqueId") or ""
        file_type = upload_info.get("fileType") or ""
        if not upload_url:
            logger.warning(
                "msteams: fileConsent/invoke missing uploadInfo.uploadUrl",
            )
            return 200

        session = await self._get_http_session()
        import aiohttp
        try:
            async with session.put(
                upload_url,
                data=pending["bytes"],
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(len(pending["bytes"])),
                    "Content-Range": f"bytes 0-{len(pending['bytes']) - 1}/{len(pending['bytes'])}",
                },
            ) as resp:
                if resp.status not in (200, 201):
                    body = await resp.text()
                    logger.warning(
                        "msteams: fileConsent upload failed: %s %s",
                        resp.status, body[:200],
                    )
                    return 200
        except aiohttp.ClientError as exc:
            logger.warning("msteams: fileConsent upload transport: %s", exc)
            return 200

        info_card = build_file_info_card(
            filename=pending["filename"],
            unique_id=unique_id,
            file_type=file_type or "file",
        )
        follow_payload: Dict[str, Any] = {
            "type": "message",
            "attachments": [info_card],
        }
        await self._post_activity(pending["chat_id"], follow_payload)
        return 200

    async def _download_bytes(self, url: str) -> Tuple[Optional[bytes], Optional[str]]:
        """Fetch *url*, returning ``(bytes, filename)`` or ``(None, None)``
        on any error.  Used when the agent emits an image URL the
        adapter wants to re-upload to SharePoint."""
        import os
        session = await self._get_http_session()
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning("msteams: image download status=%s", resp.status)
                    return None, None
                data = await resp.read()
        except Exception as exc:
            logger.warning("msteams: image download failed: %s", exc)
            return None, None
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path) or "attachment.bin"
        return data, filename

    async def _post_activity(
        self, chat_id: str, payload: Dict[str, Any],
    ) -> SendResult:
        if self._credential_provider is None:
            return SendResult(
                success=False,
                error="adapter not connected (no credential provider)",
                retryable=False,
            )
        service_url = self._service_url_for(chat_id)
        if not service_url:
            return SendResult(
                success=False,
                error=(
                    "unknown serviceUrl for conversation — Hermes must "
                    "receive at least one inbound activity from this chat "
                    "before it can send to it"
                ),
                retryable=False,
            )

        try:
            token = await self._credential_provider.get_token(BOT_FRAMEWORK_SCOPE)
        except AuthError as exc:
            return SendResult(success=False, error=str(exc), retryable=False)

        url = _activities_url(service_url, chat_id)
        session = await self._get_http_session()
        import aiohttp
        try:
            async with session.post(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json=payload,
            ) as resp:
                status = resp.status
                response_body = None
                try:
                    response_body = await resp.json(content_type=None)
                except Exception:
                    response_body = await resp.text()
                if status in (200, 201, 202):
                    message_id = None
                    if isinstance(response_body, dict):
                        message_id = response_body.get("id")
                    return SendResult(
                        success=True,
                        message_id=message_id,
                        raw_response=response_body,
                    )
                retryable = status in (408, 425, 429, 500, 502, 503, 504)
                return SendResult(
                    success=False,
                    error=f"Bot Framework {status}: {response_body}",
                    retryable=retryable,
                    raw_response=response_body,
                )
        except aiohttp.ClientError as exc:
            return SendResult(
                success=False, error=f"Bot Framework transport: {exc}", retryable=True,
            )

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return display metadata for *chat_id* using Graph when possible.

        Falls back to a minimal stub when Graph is unreachable or when
        we don't have a team id for the conversation (e.g. DMs — the
        Bot Framework doesn't hand us a Graph-queryable identifier for
        those until the user sends a message, which already captured
        chat_type via the session).
        """
        chat_type = "channel" if chat_id.startswith("19:") else "dm"
        info: Dict[str, Any] = {"name": chat_id, "type": chat_type, "chat_id": chat_id}
        if self._graph is None or chat_type != "channel":
            return info

        team_id = self._team_ids_by_chat.get(chat_id)
        if not team_id:
            return info
        channels = await self._graph.list_channels(team_id)
        for entry in channels:
            if entry.get("id") == chat_id:
                info["name"] = entry.get("display_name") or chat_id
                info["description"] = entry.get("description")
                info["membership_type"] = entry.get("membership_type")
                info["team_id"] = team_id
                break
        return info

    # ------------------------------------------------------------------
    # Graph-backed helpers (history, user resolution, uploads)
    # ------------------------------------------------------------------

    async def fetch_channel_history(
        self, team_id: str, channel_id: str, limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Oldest-first recent messages in a channel — empty if Graph
        cannot reach the conversation or the permission is missing."""
        if self._graph is None:
            return []
        top = limit if limit is not None else self._history_limit
        return await self._graph.fetch_channel_messages(team_id, channel_id, top=top)

    async def resolve_user(self, aad_object_id: str) -> Optional[Dict[str, Any]]:
        """Display name / email / role for an AAD user, or ``None``."""
        if self._graph is None or not aad_object_id:
            return None
        return await self._graph.resolve_user(aad_object_id)

    async def upload_channel_file(
        self, chat_id: str, filename: str, content: bytes,
    ) -> Optional[str]:
        """Upload *content* to the configured SharePoint site and return
        the resulting ``webUrl``.  No-op (returns ``None``) when the
        adapter has no Graph client or the site id is not configured —
        the caller downgrades to an in-text link or a plain message.
        """
        if self._graph is None or not self._sharepoint_site_id:
            return None
        # Isolate each conversation in its own folder under the bot's
        # shared SharePoint space so uploads from different channels
        # don't collide on filename.
        safe_chat_id = chat_id.replace(":", "_").replace("@", "_at_")
        folder = f"{self._sharepoint_folder}/{safe_chat_id}"
        return await self._graph.upload_to_sharepoint(
            site_id=self._sharepoint_site_id,
            folder_path=folder,
            filename=filename,
            content=content,
        )


def _activities_url(service_url: str, chat_id: str) -> str:
    """Compose ``{service_url}/v3/conversations/{chat_id}/activities``
    safely — the service URL may or may not have a trailing slash and
    may already include the ``/v3`` segment (older emulators do)."""
    base = service_url.rstrip("/")
    parsed = urlparse(base)
    segments = [s for s in parsed.path.split("/") if s]
    if "v3" not in segments:
        base = f"{base}/v3"
    # Teams conversation IDs contain ``:`` and ``@`` which aiohttp would
    # happily encode, but the Bot Framework REST API expects them raw.
    return f"{base}/conversations/{chat_id}/activities"
