"""Tests for ``gateway.platforms.msteams.cards`` and the C5 file-upload
flows in the adapter (FileConsent for DMs, file.download.info for
channels)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.msteams import adapter as msteams_adapter
from gateway.platforms.msteams.adapter import MsTeamsAdapter
from gateway.platforms.msteams.cards import (
    ADAPTIVE_CARD_CONTENT_TYPE,
    FILE_CONSENT_CONTENT_TYPE,
    FILE_DOWNLOAD_INFO_CONTENT_TYPE,
    FILE_INFO_CONTENT_TYPE,
    build_adaptive_card,
    build_file_consent_card,
    build_file_download_card,
    build_file_info_card,
    build_poll_card,
    markdown_to_teams_html,
)


# ---------------------------------------------------------------------------
# Adaptive Cards
# ---------------------------------------------------------------------------

def test_adaptive_card_wraps_body():
    card = build_adaptive_card(
        body=[{"type": "TextBlock", "text": "hi"}],
        actions=[{"type": "Action.Submit", "title": "Go"}],
    )
    assert card["contentType"] == ADAPTIVE_CARD_CONTENT_TYPE
    assert card["content"]["type"] == "AdaptiveCard"
    assert card["content"]["version"] == "1.5"
    assert card["content"]["body"] == [{"type": "TextBlock", "text": "hi"}]
    assert card["content"]["actions"][0]["title"] == "Go"


def test_adaptive_card_omits_actions_key_when_empty():
    card = build_adaptive_card(body=[{"type": "TextBlock", "text": "x"}])
    assert "actions" not in card["content"]


# ---------------------------------------------------------------------------
# Poll card
# ---------------------------------------------------------------------------

def test_poll_card_single_select_shape():
    card = build_poll_card(
        question="Best coffee?",
        options=["Latte", "Espresso", "Cortado"],
    )
    assert card["contentType"] == ADAPTIVE_CARD_CONTENT_TYPE
    body = card["content"]["body"]
    assert body[0] == {
        "type": "TextBlock", "text": "Best coffee?",
        "weight": "Bolder", "wrap": True,
    }
    choice_set = body[1]
    assert choice_set["type"] == "Input.ChoiceSet"
    assert choice_set["id"] == "choice"
    assert choice_set["style"] == "expanded"
    assert choice_set["isMultiSelect"] is False
    assert [c["value"] for c in choice_set["choices"]] == [
        "Latte", "Espresso", "Cortado",
    ]
    actions = card["content"]["actions"]
    assert actions[0]["type"] == "Action.Submit"
    assert actions[0]["title"] == "Vote"


def test_poll_card_multi_select_and_submit_data():
    card = build_poll_card(
        question="Pick toppings",
        options=["Pepperoni", "Mushrooms"],
        is_multi_select=True,
        submit_title="Order",
        submit_data={"poll_id": "p-42"},
    )
    body = card["content"]["body"]
    assert body[1]["isMultiSelect"] is True
    actions = card["content"]["actions"]
    assert actions[0]["title"] == "Order"
    assert actions[0]["data"] == {"poll_id": "p-42"}


def test_poll_card_empty_options_raises():
    with pytest.raises(ValueError, match="at least one option"):
        build_poll_card(question="?", options=[])


# ---------------------------------------------------------------------------
# FileConsentCard / FileInfoCard / file.download.info
# ---------------------------------------------------------------------------

def test_file_consent_card_shape():
    card = build_file_consent_card(
        filename="report.pdf", size_bytes=1234, description="monthly report",
    )
    assert card["contentType"] == FILE_CONSENT_CONTENT_TYPE
    assert card["name"] == "report.pdf"
    content = card["content"]
    assert content["description"] == "monthly report"
    assert content["sizeInBytes"] == 1234
    # Every consent card gets a correlation id so the invoke handler
    # can match the response back to the pending upload.
    assert "upload_id" in content["acceptContext"]
    assert isinstance(content["acceptContext"]["upload_id"], str)


def test_file_info_card_shape():
    card = build_file_info_card(
        filename="x.png", unique_id="uniq-1", file_type="png",
    )
    assert card["contentType"] == FILE_INFO_CONTENT_TYPE
    assert card["content"]["uniqueId"] == "uniq-1"
    assert card["content"]["fileType"] == "png"


def test_file_download_card_shape():
    card = build_file_download_card(
        filename="doc.pdf",
        content_url="https://sharepoint/x/doc.pdf",
    )
    assert card["contentType"] == FILE_DOWNLOAD_INFO_CONTENT_TYPE
    assert card["contentUrl"] == "https://sharepoint/x/doc.pdf"
    assert card["content"]["downloadUrl"] == "https://sharepoint/x/doc.pdf"
    assert card["content"]["fileType"] == "pdf"


def test_file_download_card_infers_file_type_for_extensionless():
    card = build_file_download_card(
        filename="LICENSE", content_url="https://x/LICENSE",
    )
    assert card["content"]["fileType"] == "file"


# ---------------------------------------------------------------------------
# markdown_to_teams_html re-export (adapter still exposes the helper)
# ---------------------------------------------------------------------------

def test_markdown_to_teams_html_still_reachable_via_adapter():
    assert msteams_adapter.markdown_to_teams_html("**x**") == "<b>x</b>"
    # Same function object — the adapter re-exports from cards.
    assert msteams_adapter.markdown_to_teams_html is markdown_to_teams_html


# ---------------------------------------------------------------------------
# Adapter file-upload flows
# ---------------------------------------------------------------------------

def _connected_adapter(**extras: Any) -> MsTeamsAdapter:
    """Build an adapter with a pre-populated serviceUrl + fake graph."""
    base = {"app_id": "a", "app_password": "p", "tenant_id": "t"}
    base.update(extras)
    adapter = MsTeamsAdapter(PlatformConfig(enabled=True, extra=base))
    adapter._credential_provider = MagicMock()
    adapter._credential_provider.get_token = AsyncMock(return_value="bearer")
    adapter._service_urls = {
        "19:chan@thread.tacv2": "https://smba.example/amer/",
        "dm-chat": "https://smba.example/amer/",
    }
    return adapter


class _FakeResponse:
    def __init__(self, status=200, payload=None):
        self.status = status
        self._payload = payload if payload is not None else {"id": "msg"}

    async def json(self, content_type=None):
        return self._payload

    async def text(self):
        return json.dumps(self._payload) if self._payload else ""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _RecordingSession:
    def __init__(self):
        self.calls = []
        self.response = _FakeResponse()
        self.closed = False

    def post(self, url, headers=None, json=None):
        self.calls.append({"method": "POST", "url": url, "headers": headers, "json": json})
        return self.response

    def put(self, url, data=None, headers=None):
        self.calls.append({"method": "PUT", "url": url, "headers": headers, "data": data})
        return _FakeResponse()

    def get(self, url):
        self.calls.append({"method": "GET", "url": url})
        return self.response

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_send_document_dm_triggers_file_consent(tmp_path, monkeypatch):
    adapter = _connected_adapter()
    fake = _RecordingSession()
    adapter._get_http_session = AsyncMock(return_value=fake)

    # Keep service_urls.json out of HERMES_HOME.
    monkeypatch.setattr(
        msteams_adapter, "_service_urls_path", lambda: tmp_path / "svc.json",
    )

    payload_path = tmp_path / "hello.txt"
    payload_path.write_bytes(b"hi hermes")

    result = await adapter.send_document("dm-chat", str(payload_path))
    assert result.success is True

    # The only POST should be the FileConsent message to the DM.
    posts = [c for c in fake.calls if c["method"] == "POST"]
    assert len(posts) == 1
    attachment = posts[0]["json"]["attachments"][0]
    assert attachment["contentType"] == FILE_CONSENT_CONTENT_TYPE
    assert attachment["name"] == "hello.txt"

    # The upload is staged — adapter waits for the invoke to complete it.
    assert len(adapter._pending_uploads) == 1
    upload_id = next(iter(adapter._pending_uploads))
    staged = adapter._pending_uploads[upload_id]
    assert staged["bytes"] == b"hi hermes"
    assert staged["chat_id"] == "dm-chat"


@pytest.mark.asyncio
async def test_file_consent_invoke_uploads_and_follows_up(tmp_path, monkeypatch):
    adapter = _connected_adapter()
    fake = _RecordingSession()
    adapter._get_http_session = AsyncMock(return_value=fake)
    monkeypatch.setattr(
        msteams_adapter, "_service_urls_path", lambda: tmp_path / "svc.json",
    )

    payload_path = tmp_path / "hello.txt"
    payload_path.write_bytes(b"payload")
    await adapter.send_document("dm-chat", str(payload_path))
    upload_id = next(iter(adapter._pending_uploads))

    activity = SimpleNamespace(
        type="invoke",
        name="fileConsent/invoke",
        value={
            "action": "accept",
            "context": {"upload_id": upload_id},
            "uploadInfo": {
                "uploadUrl": "https://onedrive.example/upload/x",
                "uniqueId": "uid-1",
                "fileType": "txt",
            },
        },
    )

    status = await adapter._handle_file_consent_invoke(activity)
    assert status == 200
    assert adapter._pending_uploads == {}

    # PUT to the upload URL
    puts = [c for c in fake.calls if c["method"] == "PUT"]
    assert puts[0]["url"] == "https://onedrive.example/upload/x"
    assert puts[0]["data"] == b"payload"

    # Follow-up POST with FileInfoCard
    posts = [c for c in fake.calls if c["method"] == "POST"]
    info_card = posts[-1]["json"]["attachments"][0]
    assert info_card["contentType"] == FILE_INFO_CONTENT_TYPE
    assert info_card["content"]["uniqueId"] == "uid-1"
    assert info_card["content"]["fileType"] == "txt"


@pytest.mark.asyncio
async def test_file_consent_invoke_decline_drops_pending(tmp_path, monkeypatch):
    adapter = _connected_adapter()
    fake = _RecordingSession()
    adapter._get_http_session = AsyncMock(return_value=fake)
    monkeypatch.setattr(
        msteams_adapter, "_service_urls_path", lambda: tmp_path / "svc.json",
    )

    payload_path = tmp_path / "hi.txt"
    payload_path.write_bytes(b"x")
    await adapter.send_document("dm-chat", str(payload_path))
    upload_id = next(iter(adapter._pending_uploads))

    activity = SimpleNamespace(
        type="invoke",
        name="fileConsent/invoke",
        value={"action": "decline", "context": {"upload_id": upload_id}},
    )

    status = await adapter._handle_file_consent_invoke(activity)
    assert status == 200
    assert adapter._pending_uploads == {}

    # No PUT, no follow-up POST — only the initial FileConsent post.
    puts = [c for c in fake.calls if c["method"] == "PUT"]
    assert puts == []


@pytest.mark.asyncio
async def test_file_consent_invoke_unknown_upload_id_is_ignored(tmp_path):
    adapter = _connected_adapter()
    fake = _RecordingSession()
    adapter._get_http_session = AsyncMock(return_value=fake)

    activity = SimpleNamespace(
        type="invoke",
        name="fileConsent/invoke",
        value={
            "action": "accept",
            "context": {"upload_id": "never-stashed"},
            "uploadInfo": {"uploadUrl": "x"},
        },
    )
    status = await adapter._handle_file_consent_invoke(activity)
    assert status == 200
    # Nothing posted or PUT.
    assert fake.calls == []


@pytest.mark.asyncio
async def test_send_document_channel_uses_sharepoint(tmp_path, monkeypatch):
    adapter = _connected_adapter(
        sharepoint_site_id="site-1", sharepoint_folder="Hermes",
    )
    adapter._team_ids_by_chat["19:chan@thread.tacv2"] = "team-1"

    fake = _RecordingSession()
    adapter._get_http_session = AsyncMock(return_value=fake)
    monkeypatch.setattr(
        msteams_adapter, "_service_urls_path", lambda: tmp_path / "svc.json",
    )

    fake_graph = MagicMock()
    fake_graph.upload_to_sharepoint = AsyncMock(
        return_value="https://sharepoint/Hermes/19_chan_at_thread.tacv2/x.pdf",
    )
    adapter._graph = fake_graph

    path = tmp_path / "report.pdf"
    path.write_bytes(b"pdfdata")
    result = await adapter.send_document("19:chan@thread.tacv2", str(path))
    assert result.success is True

    fake_graph.upload_to_sharepoint.assert_awaited_once()
    # POST to Bot Framework with a file.download.info attachment
    posts = [c for c in fake.calls if c["method"] == "POST"]
    attachment = posts[-1]["json"]["attachments"][0]
    assert attachment["contentType"] == FILE_DOWNLOAD_INFO_CONTENT_TYPE
    assert attachment["contentUrl"].startswith("https://sharepoint/")
    assert attachment["name"] == "report.pdf"


@pytest.mark.asyncio
async def test_send_document_channel_without_site_id_fails_with_clear_error(tmp_path):
    adapter = _connected_adapter()  # no sharepoint_site_id
    adapter._team_ids_by_chat["19:chan@thread.tacv2"] = "team-1"
    adapter._get_http_session = AsyncMock(return_value=_RecordingSession())

    path = tmp_path / "x.pdf"
    path.write_bytes(b"pdf")
    result = await adapter.send_document("19:chan@thread.tacv2", str(path))
    assert result.success is False
    assert "SHAREPOINT_SITE_ID" in result.error


@pytest.mark.asyncio
async def test_send_document_missing_file_returns_clear_error():
    adapter = _connected_adapter()
    result = await adapter.send_document("dm-chat", "/no/such/file.txt")
    assert result.success is False
    assert "not found" in result.error


@pytest.mark.asyncio
async def test_handle_messages_routes_invoke_to_file_consent(tmp_path, monkeypatch):
    """POST with an invoke/fileConsent activity calls the handler."""
    adapter = _connected_adapter()
    adapter._handle_file_consent_invoke = AsyncMock(return_value=200)

    # Stub JWT validation so we don't need real tokens.
    adapter._validate_jwt = AsyncMock(return_value=True)

    class _Req:
        headers: Dict[str, str] = {"Authorization": "Bearer fake"}

        async def read(self_inner):
            return json.dumps({
                "type": "invoke",
                "name": "fileConsent/invoke",
                "value": {"action": "accept", "context": {"upload_id": "uid"}},
                "serviceUrl": "https://smba.example/amer/",
                "conversation": {"id": "dm-chat"},
                "from": {"id": "user-1"},
            }).encode("utf-8")

    resp = await adapter._handle_messages(_Req())
    assert resp.status == 200
    adapter._handle_file_consent_invoke.assert_awaited_once()
