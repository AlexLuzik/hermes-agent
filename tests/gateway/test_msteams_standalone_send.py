"""Tests for the gateway-less ``_send_msteams`` helper used by cron jobs
and the ``send_message`` tool when the Hermes gateway process isn't
running.  Exercises the three contract requirements:

- resolve ``serviceUrl`` from ``$HERMES_HOME/msteams/service_urls.json``
- mint a Bot Framework bearer token via MSAL client-credentials
- POST the activity to ``{serviceUrl}/v3/conversations/{chat_id}/activities``

All network I/O is mocked — the test never contacts Azure or a live Bot
Framework endpoint.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sidecar_dir(tmp_path, monkeypatch):
    """Point ``HERMES_HOME`` at a tmp dir with a pre-seeded sidecar."""
    ms_dir = tmp_path / "msteams"
    ms_dir.mkdir()
    (ms_dir / "service_urls.json").write_text(json.dumps({
        "19:chat@thread.tacv2": "https://smba.example/amer/",
        "dm-chat": "https://smba.example/amer/",
    }), encoding="utf-8")
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)
    return tmp_path


@pytest.fixture
def msteams_env(monkeypatch):
    monkeypatch.setenv("MSTEAMS_APP_ID", "app-123")
    monkeypatch.setenv("MSTEAMS_APP_PASSWORD", "s3cret")
    monkeypatch.setenv("MSTEAMS_TENANT_ID", "tenant-xyz")


# ---------------------------------------------------------------------------
# aiohttp-ish mocks
# ---------------------------------------------------------------------------

class _FakeResp:
    def __init__(self, status=200, payload=None):
        self.status = status
        self._payload = payload if payload is not None else {"id": "msg-1"}

    async def json(self, content_type=None):
        return self._payload

    async def text(self):
        return json.dumps(self._payload) if self._payload else ""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    def __init__(self, response):
        self._response = response
        self.posts: List[Dict[str, Any]] = []

    def post(self, url, headers=None, json=None):
        self.posts.append({"url": url, "headers": headers, "json": json})
        return self._response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.fixture
def patched_aiohttp(monkeypatch):
    """Patch aiohttp.ClientSession / ClientTimeout with a recording fake."""
    fake_session = _FakeSession(_FakeResp())

    class _FakeTimeout:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    monkeypatch.setattr("aiohttp.ClientSession", lambda timeout=None: fake_session)
    monkeypatch.setattr("aiohttp.ClientTimeout", _FakeTimeout)
    return fake_session


@pytest.fixture
def patched_msal(monkeypatch):
    """Stub msal.ConfidentialClientApplication to return a fixed token."""
    captured: Dict[str, Any] = {}

    class _FakeApp:
        def __init__(self, client_id, authority, client_credential):
            captured["client_id"] = client_id
            captured["authority"] = authority
            captured["credential"] = client_credential

        def acquire_token_for_client(self, scopes):
            captured["scopes"] = list(scopes)
            return {"access_token": "bf-standalone-tok", "expires_in": 3600}

    import msal
    monkeypatch.setattr(msal, "ConfidentialClientApplication", _FakeApp)
    return captured


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_msteams_happy_path(
    sidecar_dir, msteams_env, patched_msal, patched_aiohttp,
):
    from tools.send_message_tool import _send_msteams

    result = await _send_msteams({}, "dm-chat", "**hello** world")

    assert result["success"] is True
    assert result["platform"] == "msteams"
    assert result["chat_id"] == "dm-chat"
    assert result["message_id"] == "msg-1"

    # MSAL args shape
    assert patched_msal["client_id"] == "app-123"
    assert patched_msal["credential"] == "s3cret"
    assert patched_msal["authority"] == (
        "https://login.microsoftonline.com/tenant-xyz"
    )
    assert patched_msal["scopes"] == ["https://api.botframework.com/.default"]

    # HTTP POST shape
    call = patched_aiohttp.posts[0]
    assert call["url"] == (
        "https://smba.example/amer/v3/conversations/dm-chat/activities"
    )
    assert call["headers"]["Authorization"] == "Bearer bf-standalone-tok"
    assert call["json"]["type"] == "message"
    assert call["json"]["textFormat"] == "xml"
    assert call["json"]["text"] == "<b>hello</b> world"


@pytest.mark.asyncio
async def test_send_msteams_reply_in_thread(
    sidecar_dir, msteams_env, patched_msal, patched_aiohttp,
):
    from tools.send_message_tool import _send_msteams
    await _send_msteams({}, "dm-chat", "ok", thread_id="act-parent-1")
    assert patched_aiohttp.posts[0]["json"]["replyToId"] == "act-parent-1"


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_msteams_unknown_chat(
    sidecar_dir, msteams_env, patched_msal, patched_aiohttp,
):
    from tools.send_message_tool import _send_msteams
    result = await _send_msteams({}, "never-seen", "hi")
    assert "error" in result
    assert "cached serviceUrl" in result["error"]


@pytest.mark.asyncio
async def test_send_msteams_missing_credentials(sidecar_dir, monkeypatch):
    # Explicitly unset — other tests could have leaked these
    monkeypatch.delenv("MSTEAMS_APP_ID", raising=False)
    monkeypatch.delenv("MSTEAMS_APP_PASSWORD", raising=False)
    from tools.send_message_tool import _send_msteams
    result = await _send_msteams({}, "dm-chat", "hi")
    assert "error" in result
    assert "MSTEAMS_APP_ID" in result["error"]


@pytest.mark.asyncio
async def test_send_msteams_no_sidecar(tmp_path, msteams_env, monkeypatch):
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)
    from tools.send_message_tool import _send_msteams
    result = await _send_msteams({}, "dm-chat", "hi")
    assert "error" in result
    assert "No MSTeams conversations cached" in result["error"]


@pytest.mark.asyncio
async def test_send_msteams_msal_failure(
    sidecar_dir, msteams_env, patched_aiohttp, monkeypatch,
):
    class _BadApp:
        def __init__(self, *args, **kwargs): ...
        def acquire_token_for_client(self, scopes):
            return {
                "error": "invalid_client",
                "error_description": "AADSTS700…: bad secret",
            }
    import msal
    monkeypatch.setattr(msal, "ConfidentialClientApplication", _BadApp)
    from tools.send_message_tool import _send_msteams
    result = await _send_msteams({}, "dm-chat", "hi")
    assert "error" in result
    assert "bad secret" in result["error"]


@pytest.mark.asyncio
async def test_send_msteams_5xx_reports_status(
    sidecar_dir, msteams_env, patched_msal, monkeypatch,
):
    session = _FakeSession(_FakeResp(status=503, payload={"error": "overloaded"}))
    monkeypatch.setattr("aiohttp.ClientSession", lambda timeout=None: session)
    monkeypatch.setattr(
        "aiohttp.ClientTimeout", lambda **kw: MagicMock(),
    )
    from tools.send_message_tool import _send_msteams
    result = await _send_msteams({}, "dm-chat", "hi")
    assert "error" in result
    assert "503" in result["error"]
