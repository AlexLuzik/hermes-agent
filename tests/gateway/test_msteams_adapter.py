"""Tests for the MS Teams adapter's C3 protocol layer.

Covers the markdown→HTML converter, mention stripping, activity parsing,
policy gates (dm_policy / allow_from / require_mention / per-team
overrides), the service-URL sidecar, the outbound POST URL composition,
and the ``send`` / ``send_typing`` code paths with a mocked aiohttp
response.  Each test sets up only the minimum state required — no real
Bot Framework or HTTPS calls are made.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import MessageType
from gateway.platforms.msteams import adapter as msteams_adapter
from gateway.platforms.msteams.adapter import (
    MsTeamsAdapter,
    _activities_url,
    markdown_to_teams_html,
    strip_bot_mention,
)


# ---------------------------------------------------------------------------
# markdown_to_teams_html
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "source,expected",
    [
        ("", ""),
        ("plain text", "plain text"),
        ("**bold**", "<b>bold</b>"),
        ("*italic*", "<i>italic</i>"),
        ("_italic_", "<i>italic</i>"),
        ("mixed **b** and *i*", "mixed <b>b</b> and <i>i</i>"),
        ("`code`", "<code>code</code>"),
        ("[link](https://x)", '<a href="https://x">link</a>'),
        ("<script>x</script>", "&lt;script&gt;x&lt;/script&gt;"),
        ("line1\nline2", "line1<br>line2"),
        ("para1\n\npara2", "para1<br><br>para2"),
    ],
)
def test_markdown_to_teams_html_shapes(source, expected):
    assert markdown_to_teams_html(source) == expected


def test_markdown_to_teams_html_fenced_code_block():
    src = "before\n```python\nx = 1\n```\nafter"
    result = markdown_to_teams_html(src)
    assert "<pre><code>x = 1</code></pre>" in result
    # Fenced-block body is escaped *once*, not twice.
    src = "```\n<b>notbold</b>\n```"
    result = markdown_to_teams_html(src)
    assert "<pre><code>&lt;b&gt;notbold&lt;/b&gt;</code></pre>" in result


def test_markdown_to_teams_html_lists():
    result = markdown_to_teams_html("- one\n- two\n- three")
    assert result == "<ul><li>one</li><li>two</li><li>three</li></ul>"

    result = markdown_to_teams_html("1. first\n2. second")
    assert result == "<ol><li>first</li><li>second</li></ol>"


def test_markdown_to_teams_html_preserves_inline_code_contents():
    """`**foo**` inside inline code should stay literal, not bold."""
    result = markdown_to_teams_html("normal **bold** `**not-bold**`")
    assert "<b>bold</b>" in result
    assert "<code>**not-bold**</code>" in result


# ---------------------------------------------------------------------------
# strip_bot_mention
# ---------------------------------------------------------------------------

def test_strip_bot_mention_removes_at_tag():
    cleaned, mentioned = strip_bot_mention(
        "<at>Hermes</at> please refactor this", "app-id", "Hermes",
    )
    assert mentioned is True
    assert cleaned == "please refactor this"


def test_strip_bot_mention_plain_at_prefix():
    cleaned, mentioned = strip_bot_mention(
        "@Hermes do the thing", "app-id", "Hermes",
    )
    assert mentioned is True
    assert cleaned == "do the thing"


def test_strip_bot_mention_not_mentioned_leaves_text_intact():
    cleaned, mentioned = strip_bot_mention(
        "hello world", "app-id", "Hermes",
    )
    assert mentioned is False
    assert cleaned == "hello world"


def test_strip_bot_mention_matches_by_app_id_inside_tag():
    cleaned, mentioned = strip_bot_mention(
        "<at>APP-ID</at> hi", "app-id", "DifferentName",
    )
    assert mentioned is True
    assert cleaned == "hi"


# ---------------------------------------------------------------------------
# _activities_url
# ---------------------------------------------------------------------------

def test_activities_url_adds_v3_when_missing():
    assert (
        _activities_url("https://smba.trafficmanager.net/amer/", "19:abc@thread.tacv2")
        == "https://smba.trafficmanager.net/amer/v3/conversations/19:abc@thread.tacv2/activities"
    )


def test_activities_url_preserves_v3_when_present():
    assert (
        _activities_url("https://smba.trafficmanager.net/amer/v3", "chat1")
        == "https://smba.trafficmanager.net/amer/v3/conversations/chat1/activities"
    )


def test_activities_url_strips_trailing_slash():
    assert (
        _activities_url("https://x.example/", "chat1")
        == "https://x.example/v3/conversations/chat1/activities"
    )


# ---------------------------------------------------------------------------
# Adapter construction & lifecycle
# ---------------------------------------------------------------------------

def _config(**extras: Any) -> PlatformConfig:
    base = {"app_id": "app-id", "app_password": "secret", "tenant_id": "t"}
    base.update(extras)
    return PlatformConfig(enabled=True, extra=base)


def test_adapter_reads_extra_defaults():
    adapter = MsTeamsAdapter(_config())
    assert adapter._host == "0.0.0.0"
    assert adapter._port == 3978
    assert adapter._path == "/api/messages"
    assert adapter._require_mention is True
    assert adapter._reply_style == "thread"
    assert adapter._dm_policy == "pairing"


def test_adapter_reads_teams_overrides():
    adapter = MsTeamsAdapter(_config(teams={
        "team-1": {
            "require_mention": False,
            "reply_style": "top-level",
            "allow_from": ["aad-u1"],
            "channels": {
                "ch-1": {"require_mention": True, "allow_from": ["aad-admin"]},
            },
        },
    }))
    eff = adapter._effective_policy(team_id="team-1", channel_id=None)
    assert eff["require_mention"] is False
    assert eff["reply_style"] == "top-level"
    assert eff["allow_from"] == ["aad-u1"]

    # Channel override wins over team default
    eff2 = adapter._effective_policy(team_id="team-1", channel_id="ch-1")
    assert eff2["require_mention"] is True
    assert eff2["allow_from"] == ["aad-admin"]


def test_adapter_effective_policy_unknown_team_uses_defaults():
    adapter = MsTeamsAdapter(_config(require_mention=False, reply_style="top-level"))
    eff = adapter._effective_policy(team_id="unknown", channel_id=None)
    assert eff["require_mention"] is False
    assert eff["reply_style"] == "top-level"


# ---------------------------------------------------------------------------
# Policy gates
# ---------------------------------------------------------------------------

def _effective(adapter: MsTeamsAdapter) -> Dict[str, Any]:
    return adapter._effective_policy(None, None)


def test_dm_policy_disabled_rejects():
    adapter = MsTeamsAdapter(_config(dm_policy="disabled"))
    ok, reason = adapter._policy_check(
        chat_type="dm", user_id="u", chat_id="c", mentioned=False,
        effective=_effective(adapter),
    )
    assert ok is False
    assert "disabled" in reason


def test_dm_policy_open_accepts_anyone():
    adapter = MsTeamsAdapter(_config(dm_policy="open"))
    ok, _ = adapter._policy_check(
        chat_type="dm", user_id="stranger", chat_id="c", mentioned=False,
        effective=_effective(adapter),
    )
    assert ok is True


def test_dm_policy_allowlist_requires_membership():
    adapter = MsTeamsAdapter(_config(dm_policy="allowlist", allow_from=["aad-u1"]))
    assert adapter._policy_check(
        chat_type="dm", user_id="aad-u1", chat_id="c", mentioned=False,
        effective=_effective(adapter),
    )[0] is True
    assert adapter._policy_check(
        chat_type="dm", user_id="stranger", chat_id="c", mentioned=False,
        effective=_effective(adapter),
    )[0] is False


def test_channel_require_mention_drops_un_mentioned():
    adapter = MsTeamsAdapter(_config(require_mention=True))
    assert adapter._policy_check(
        chat_type="channel", user_id="u", chat_id="ch",
        mentioned=False, effective=_effective(adapter),
    )[0] is False
    assert adapter._policy_check(
        chat_type="channel", user_id="u", chat_id="ch",
        mentioned=True, effective=_effective(adapter),
    )[0] is True


def test_channel_free_response_channels_bypass_mention():
    adapter = MsTeamsAdapter(_config(
        require_mention=True, free_response_channels=["ch-free"],
    ))
    assert adapter._policy_check(
        chat_type="channel", user_id="u", chat_id="ch-free",
        mentioned=False, effective=_effective(adapter),
    )[0] is True


def test_group_allow_from_blocks_non_members():
    adapter = MsTeamsAdapter(_config(
        group_allow_from=["aad-allowed"], require_mention=False,
    ))
    assert adapter._policy_check(
        chat_type="group", user_id="aad-allowed", chat_id="g",
        mentioned=False, effective=_effective(adapter),
    )[0] is True
    assert adapter._policy_check(
        chat_type="group", user_id="aad-stranger", chat_id="g",
        mentioned=False, effective=_effective(adapter),
    )[0] is False


# ---------------------------------------------------------------------------
# _build_event
# ---------------------------------------------------------------------------

def _activity(**overrides):
    """Return a duck-typed object mirroring botbuilder's Activity."""
    conv = SimpleNamespace(
        id=overrides.get("conv_id", "19:room@thread.tacv2"),
        conversation_type=overrides.get("conv_type", "personal"),
        name=overrides.get("conv_name"),
    )
    from_ = SimpleNamespace(
        id=overrides.get("from_id", "29:user-guid"),
        aad_object_id=overrides.get("aad_id", "aad-user"),
        name=overrides.get("from_name", "Alice"),
    )
    return SimpleNamespace(
        type="message",
        text=overrides.get("text", "hello"),
        id=overrides.get("activity_id", "act-1"),
        reply_to_id=overrides.get("reply_to", None),
        service_url=overrides.get("service_url", "https://smba.example/amer/"),
        conversation=conv,
        from_property=from_,
        channel_data=overrides.get("channel_data", {}),
    )


def test_build_event_dm_accepts():
    adapter = MsTeamsAdapter(_config(dm_policy="open"))
    event, dispatch = adapter._build_event(_activity())
    assert dispatch is True
    assert event.text == "hello"
    assert event.source.chat_type == "dm"
    assert event.source.user_id == "aad-user"
    assert event.message_id == "act-1"


def test_build_event_channel_requires_mention():
    adapter = MsTeamsAdapter(_config(require_mention=True, bot_display_name="Hermes"))
    activity = _activity(
        conv_id="19:ch@thread.tacv2",
        conv_type="channel",
        text="hello without ping",
        channel_data={"team": {"id": "team-1"}, "channel": {"id": "19:ch@thread.tacv2"}},
    )
    event, dispatch = adapter._build_event(activity)
    assert dispatch is False
    assert event is None

    # With a mention, it goes through and the <at> tag is stripped.
    activity.text = "<at>Hermes</at> please"
    event, dispatch = adapter._build_event(activity)
    assert dispatch is True
    assert event.text == "please"
    assert event.source.chat_type == "channel"
    assert event.source.chat_id_alt == "team-1"  # team id captured


def test_build_event_group_uses_group_allowlist():
    adapter = MsTeamsAdapter(_config(
        require_mention=False, group_allow_from=["aad-allowed"],
    ))
    activity = _activity(conv_type="groupChat", aad_id="aad-allowed")
    event, dispatch = adapter._build_event(activity)
    assert dispatch is True
    assert event.source.chat_type == "group"


def test_build_event_empty_text_after_mention_strip_is_dropped():
    adapter = MsTeamsAdapter(_config(require_mention=True, bot_display_name="Hermes"))
    activity = _activity(
        conv_type="channel",
        text="<at>Hermes</at>",
        channel_data={"team": {"id": "team-1"}, "channel": {"id": "c1"}},
    )
    event, dispatch = adapter._build_event(activity)
    assert dispatch is False


# ---------------------------------------------------------------------------
# Service URL persistence
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remember_service_url_writes_sidecar(monkeypatch, tmp_path):
    monkeypatch.setattr(
        msteams_adapter, "_service_urls_path",
        lambda: tmp_path / "msteams" / "service_urls.json",
    )
    (tmp_path / "msteams").mkdir(parents=True, exist_ok=True)
    adapter = MsTeamsAdapter(_config())
    adapter._service_urls = {}
    await adapter._remember_service_url("chat1", "https://smba.example/amer/")
    data = json.loads((tmp_path / "msteams" / "service_urls.json").read_text())
    assert data == {"chat1": "https://smba.example/amer/"}


@pytest.mark.asyncio
async def test_remember_service_url_skips_unchanged(monkeypatch, tmp_path):
    path = tmp_path / "msteams" / "service_urls.json"
    monkeypatch.setattr(msteams_adapter, "_service_urls_path", lambda: path)
    adapter = MsTeamsAdapter(_config())
    adapter._service_urls = {"chat1": "https://smba.example/amer/"}
    # No write should occur.
    await adapter._remember_service_url("chat1", "https://smba.example/amer/")
    assert not path.exists()


# ---------------------------------------------------------------------------
# _post_activity — outbound happy path / error paths
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, status=200, payload=None):
        self.status = status
        self._payload = payload if payload is not None else {"id": "msg-1"}

    async def json(self, content_type=None):
        return self._payload

    async def text(self):
        return json.dumps(self._payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, response):
        self._response = response
        self.calls: List[Dict[str, Any]] = []
        self.closed = False

    def post(self, url, headers=None, json=None):
        self.calls.append({"url": url, "headers": headers, "json": json})
        return self._response

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_send_happy_path(monkeypatch):
    adapter = MsTeamsAdapter(_config())
    adapter._credential_provider = MagicMock()
    adapter._credential_provider.get_token = AsyncMock(return_value="bearer-tok")
    adapter._service_urls = {"chat1": "https://smba.example/amer/"}
    fake_session = _FakeSession(_FakeResponse(status=201, payload={"id": "new-msg"}))

    async def _get_session():
        return fake_session

    adapter._get_http_session = _get_session

    result = await adapter.send("chat1", "**hi** world")
    assert result.success is True
    assert result.message_id == "new-msg"

    call = fake_session.calls[0]
    assert call["url"] == "https://smba.example/amer/v3/conversations/chat1/activities"
    assert call["headers"]["Authorization"] == "Bearer bearer-tok"
    assert call["json"]["type"] == "message"
    assert call["json"]["textFormat"] == "xml"
    assert call["json"]["text"] == "<b>hi</b> world"


@pytest.mark.asyncio
async def test_send_without_service_url_reports_clear_error():
    adapter = MsTeamsAdapter(_config())
    adapter._credential_provider = MagicMock()
    adapter._service_urls = {}  # nothing cached yet
    result = await adapter.send("unknown-chat", "hi")
    assert result.success is False
    assert "serviceUrl" in result.error
    assert result.retryable is False


@pytest.mark.asyncio
async def test_send_propagates_auth_error():
    from gateway.platforms.msteams.auth import AuthError
    adapter = MsTeamsAdapter(_config())
    adapter._credential_provider = MagicMock()
    adapter._credential_provider.get_token = AsyncMock(side_effect=AuthError("nope"))
    adapter._service_urls = {"chat1": "https://x/"}
    result = await adapter.send("chat1", "hi")
    assert result.success is False
    assert result.retryable is False
    assert "nope" in result.error


@pytest.mark.asyncio
async def test_send_marks_5xx_retryable(monkeypatch):
    adapter = MsTeamsAdapter(_config())
    adapter._credential_provider = MagicMock()
    adapter._credential_provider.get_token = AsyncMock(return_value="tok")
    adapter._service_urls = {"c": "https://x/"}
    fake_session = _FakeSession(_FakeResponse(status=503, payload={"error": "boom"}))

    async def _get_session():
        return fake_session

    adapter._get_http_session = _get_session

    result = await adapter.send("c", "hi")
    assert result.success is False
    assert result.retryable is True


@pytest.mark.asyncio
async def test_send_typing_posts_typing_activity():
    adapter = MsTeamsAdapter(_config())
    adapter._credential_provider = MagicMock()
    adapter._credential_provider.get_token = AsyncMock(return_value="tok")
    adapter._service_urls = {"c": "https://x/"}
    fake_session = _FakeSession(_FakeResponse())

    async def _get_session():
        return fake_session

    adapter._get_http_session = _get_session

    await adapter.send_typing("c")
    assert fake_session.calls[0]["json"] == {"type": "typing"}


# ---------------------------------------------------------------------------
# connect() failure paths
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_without_app_id_fails():
    adapter = MsTeamsAdapter(PlatformConfig(enabled=True, extra={}))
    assert await adapter.connect() is False
    assert adapter.has_fatal_error
    assert adapter.fatal_error_code == "msteams_config"


@pytest.mark.asyncio
async def test_connect_without_password_fails_with_auth_error():
    # Secret auth but no password — auth.py raises AuthError which
    # connect() converts into a fatal state.
    adapter = MsTeamsAdapter(PlatformConfig(enabled=True, extra={"app_id": "x"}))
    assert await adapter.connect() is False
    assert adapter.has_fatal_error
    assert adapter.fatal_error_code == "msteams_auth"
