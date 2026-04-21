"""Tests for ``gateway.platforms.msteams.graph`` (C4).

Exercises the Graph client wrapper without making real HTTPS calls.
Each test injects a fake ``client`` attribute (mirroring the
``msgraph.GraphServiceClient`` interface just enough for the method
under test) so the SDK's transport, Kiota plumbing, and Azure auth
stay out of the loop.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.platforms.msteams import graph as graph_module
from gateway.platforms.msteams.auth import GRAPH_SCOPE, AuthError
from gateway.platforms.msteams.graph import (
    GraphClient,
    _HermesTokenCredential,
    _attr,
    _message_to_dict,
)


# ---------------------------------------------------------------------------
# _HermesTokenCredential
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hermes_token_credential_delegates_to_provider():
    provider = MagicMock()
    provider.get_token = AsyncMock(return_value="delegated")
    cred = _HermesTokenCredential(provider)
    result = await cred.get_token(GRAPH_SCOPE)
    assert result.token == "delegated"
    assert result.expires_on > 0
    provider.get_token.assert_awaited_once_with(GRAPH_SCOPE)


@pytest.mark.asyncio
async def test_hermes_token_credential_requires_scope():
    cred = _HermesTokenCredential(MagicMock())
    with pytest.raises(AuthError):
        await cred.get_token()


# ---------------------------------------------------------------------------
# _attr + _message_to_dict
# ---------------------------------------------------------------------------

def test_attr_reads_attributes_and_dicts():
    assert _attr(SimpleNamespace(id="x"), "id") == "x"
    assert _attr({"id": "x"}, "id") == "x"
    assert _attr(None, "id") is None
    assert _attr(SimpleNamespace(), "missing", default=7) == 7


def test_message_to_dict_basic_shape():
    msg = SimpleNamespace(
        id="msg-1",
        created_date_time="2026-04-21T10:00",
        body=SimpleNamespace(content="hello <b>world</b>", content_type="html"),
        from_=SimpleNamespace(user=SimpleNamespace(id="u1", display_name="Alice")),
        reply_to_id=None,
    )
    out = _message_to_dict(msg)
    assert out == {
        "id": "msg-1",
        "created_date_time": "2026-04-21T10:00",
        "text": "hello <b>world</b>",
        "content_type": "html",
        "from_id": "u1",
        "from_name": "Alice",
        "reply_to_id": None,
    }


def test_message_to_dict_handles_missing_from():
    msg = SimpleNamespace(
        id="m",
        created_date_time=None,
        body=SimpleNamespace(content="", content_type="text"),
        from_=None,
        reply_to_id="parent-1",
    )
    out = _message_to_dict(msg)
    assert out["from_id"] is None
    assert out["from_name"] is None
    assert out["reply_to_id"] == "parent-1"


# ---------------------------------------------------------------------------
# GraphClient — fake SDK client fixture
# ---------------------------------------------------------------------------

def _install_fake_client(monkeypatch, graph_client: GraphClient, fake):
    """Inject *fake* as GraphClient's cached ``_client``."""
    graph_client._client = fake


class _FakeUserCollection:
    def __init__(self, users):
        self._users = users
        self.last_config = None

    async def get(self, request_configuration=None):
        self.last_config = request_configuration
        return SimpleNamespace(value=self._users)


class _FakeUserItem:
    def __init__(self, user):
        self._user = user

    async def get(self):
        return self._user


class _FakeUsers:
    def __init__(self, collection, items):
        self._collection = collection
        self._items = items

    def by_user_id(self, user_id):
        if user_id not in self._items:
            raise KeyError(user_id)
        return _FakeUserItem(self._items[user_id])

    async def get(self, request_configuration=None):
        return await self._collection.get(request_configuration=request_configuration)


@pytest.mark.asyncio
async def test_resolve_user_returns_dict(monkeypatch):
    fake_user = SimpleNamespace(
        id="aad-1",
        display_name="Alice",
        mail="alice@example.com",
        user_principal_name="alice@corp.onmicrosoft.com",
        job_title="Engineer",
    )
    fake_client = SimpleNamespace(users=_FakeUsers(
        collection=_FakeUserCollection([]),
        items={"aad-1": fake_user},
    ))

    gc = GraphClient(provider=MagicMock())
    _install_fake_client(monkeypatch, gc, fake_client)

    result = await gc.resolve_user("aad-1")
    assert result == {
        "id": "aad-1",
        "display_name": "Alice",
        "email": "alice@example.com",
        "job_title": "Engineer",
    }


@pytest.mark.asyncio
async def test_resolve_user_returns_none_on_error(monkeypatch):
    class _Exploding:
        def by_user_id(self, user_id):
            raise RuntimeError("401 unauthorized")

    fake_client = SimpleNamespace(users=_Exploding())
    gc = GraphClient(provider=MagicMock())
    _install_fake_client(monkeypatch, gc, fake_client)
    assert await gc.resolve_user("x") is None


@pytest.mark.asyncio
async def test_search_users_by_display_name(monkeypatch):
    fake_users = [
        SimpleNamespace(id="1", display_name="Alice", mail="a@x", user_principal_name=None),
        SimpleNamespace(id="2", display_name="Alistair", mail=None, user_principal_name="al@x"),
    ]
    coll = _FakeUserCollection(fake_users)
    fake_client = SimpleNamespace(users=_FakeUsers(collection=coll, items={}))
    gc = GraphClient(provider=MagicMock())
    _install_fake_client(monkeypatch, gc, fake_client)

    results = await gc.search_users_by_display_name("Ali", top=2)
    assert [u["display_name"] for u in results] == ["Alice", "Alistair"]
    # Confirm prefix makes it into the query — the request configuration
    # was recorded by the fake.
    assert coll.last_config is not None
    query = getattr(coll.last_config, "query_parameters", None)
    filt = getattr(query, "filter", "") if query else ""
    assert "startswith" in filt
    assert "Ali" in filt


@pytest.mark.asyncio
async def test_search_users_escapes_single_quote(monkeypatch):
    coll = _FakeUserCollection([])
    fake_client = SimpleNamespace(users=_FakeUsers(collection=coll, items={}))
    gc = GraphClient(provider=MagicMock())
    _install_fake_client(monkeypatch, gc, fake_client)

    await gc.search_users_by_display_name("O'Neil")
    filt = coll.last_config.query_parameters.filter
    assert "O''Neil" in filt
    assert "O'Neil'" not in filt.replace("O''Neil", "")


# ---------------------------------------------------------------------------
# fetch_channel_messages
# ---------------------------------------------------------------------------

class _FakeMessages:
    def __init__(self, messages):
        self._messages = messages
        self.last_config = None

    async def get(self, request_configuration=None):
        self.last_config = request_configuration
        return SimpleNamespace(value=self._messages)


class _FakeChannelItem:
    def __init__(self, messages):
        self.messages = messages


class _FakeChannels:
    def __init__(self, items):
        self._items = items

    def by_channel_id(self, channel_id):
        return self._items[channel_id]


class _FakeTeamItem:
    def __init__(self, channels):
        self.channels = channels


class _FakeTeams:
    def __init__(self, items):
        self._items = items

    def by_team_id(self, team_id):
        return self._items[team_id]


def _make_channel_history_client(messages):
    msgs = _FakeMessages(messages)
    channel = _FakeChannelItem(messages=msgs)
    channels = _FakeChannels(items={"ch-1": channel})
    team = _FakeTeamItem(channels=channels)
    teams = _FakeTeams(items={"team-1": team})
    return SimpleNamespace(teams=teams), msgs


@pytest.mark.asyncio
async def test_fetch_channel_messages_reverses_to_oldest_first(monkeypatch):
    m1 = SimpleNamespace(
        id="m1", created_date_time=None,
        body=SimpleNamespace(content="older", content_type="text"),
        from_=None, reply_to_id=None,
    )
    m2 = SimpleNamespace(
        id="m2", created_date_time=None,
        body=SimpleNamespace(content="newer", content_type="text"),
        from_=None, reply_to_id=None,
    )
    # Graph returns newest-first; we expect the wrapper to reverse it.
    fake_client, _ = _make_channel_history_client([m2, m1])
    gc = GraphClient(provider=MagicMock())
    _install_fake_client(monkeypatch, gc, fake_client)

    history = await gc.fetch_channel_messages("team-1", "ch-1", top=25)
    assert [m["id"] for m in history] == ["m1", "m2"]


@pytest.mark.asyncio
async def test_fetch_channel_messages_returns_empty_on_error(monkeypatch):
    class _Failing:
        def by_team_id(self, _id):
            raise RuntimeError("permission denied")

    fake_client = SimpleNamespace(teams=_Failing())
    gc = GraphClient(provider=MagicMock())
    _install_fake_client(monkeypatch, gc, fake_client)

    assert await gc.fetch_channel_messages("t", "c") == []


# ---------------------------------------------------------------------------
# list_joined_teams + list_channels
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_joined_teams(monkeypatch):
    class _FakeMe:
        class joined_teams:
            @staticmethod
            async def get():
                return SimpleNamespace(value=[
                    SimpleNamespace(id="t1", display_name="Team One", description="d1"),
                ])

    fake_client = SimpleNamespace(me=_FakeMe())
    gc = GraphClient(provider=MagicMock())
    _install_fake_client(monkeypatch, gc, fake_client)

    teams = await gc.list_joined_teams()
    assert teams == [{"id": "t1", "display_name": "Team One", "description": "d1"}]


@pytest.mark.asyncio
async def test_list_channels(monkeypatch):
    class _FakeChannelList:
        async def get(self):
            return SimpleNamespace(value=[
                SimpleNamespace(
                    id="ch", display_name="General",
                    description="the general channel",
                    membership_type="standard",
                ),
            ])

    class _FakeTeamItemC:
        channels = _FakeChannelList()

    fake_client = SimpleNamespace(teams=_FakeTeams(items={"t1": _FakeTeamItemC()}))
    gc = GraphClient(provider=MagicMock())
    _install_fake_client(monkeypatch, gc, fake_client)

    channels = await gc.list_channels("t1")
    assert channels[0]["display_name"] == "General"
    assert channels[0]["membership_type"] == "standard"


# ---------------------------------------------------------------------------
# download_hosted_content
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_download_hosted_content_returns_bytes(monkeypatch):
    class _Content:
        async def get(self):
            return b"JPEGDATA"

    class _HostedItem:
        content = _Content()

    class _Hosted:
        def by_chat_message_hosted_content_id(self, _hc):
            return _HostedItem()

    class _MessageItem:
        hosted_contents = _Hosted()

    class _Messages:
        def by_chat_message_id(self, _mid):
            return _MessageItem()

    class _ChItem:
        messages = _Messages()

    class _Channels:
        def by_channel_id(self, _cid):
            return _ChItem()

    class _TmItem:
        channels = _Channels()

    fake_client = SimpleNamespace(teams=_FakeTeams(items={"t": _TmItem()}))
    gc = GraphClient(provider=MagicMock())
    _install_fake_client(monkeypatch, gc, fake_client)

    data = await gc.download_hosted_content("t", "c", "m", "hc-1")
    assert data == b"JPEGDATA"


@pytest.mark.asyncio
async def test_download_hosted_content_returns_none_on_error(monkeypatch):
    class _Failing:
        def by_team_id(self, _):
            raise RuntimeError("gone")

    gc = GraphClient(provider=MagicMock())
    _install_fake_client(monkeypatch, gc, SimpleNamespace(teams=_Failing()))

    assert await gc.download_hosted_content("t", "c", "m", "h") is None


# ---------------------------------------------------------------------------
# Adapter integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_adapter_get_chat_info_uses_graph(monkeypatch):
    from gateway.platforms.msteams.adapter import MsTeamsAdapter
    from gateway.config import PlatformConfig

    adapter = MsTeamsAdapter(PlatformConfig(
        enabled=True,
        extra={"app_id": "a", "app_password": "p", "tenant_id": "t"},
    ))
    adapter._team_ids_by_chat["19:chan@thread.tacv2"] = "team-1"

    fake_graph = MagicMock()
    fake_graph.list_channels = AsyncMock(return_value=[
        {"id": "19:chan@thread.tacv2", "display_name": "General",
         "description": "welcome", "membership_type": "standard"},
    ])
    adapter._graph = fake_graph

    info = await adapter.get_chat_info("19:chan@thread.tacv2")
    assert info["name"] == "General"
    assert info["team_id"] == "team-1"
    assert info["type"] == "channel"


@pytest.mark.asyncio
async def test_adapter_get_chat_info_without_graph_falls_back():
    from gateway.platforms.msteams.adapter import MsTeamsAdapter
    from gateway.config import PlatformConfig

    adapter = MsTeamsAdapter(PlatformConfig(
        enabled=True,
        extra={"app_id": "a", "app_password": "p", "tenant_id": "t"},
    ))
    info = await adapter.get_chat_info("19:chan@thread.tacv2")
    assert info == {
        "name": "19:chan@thread.tacv2",
        "type": "channel",
        "chat_id": "19:chan@thread.tacv2",
    }


@pytest.mark.asyncio
async def test_adapter_fetch_channel_history_defaults_to_history_limit():
    from gateway.platforms.msteams.adapter import MsTeamsAdapter
    from gateway.config import PlatformConfig

    adapter = MsTeamsAdapter(PlatformConfig(
        enabled=True,
        extra={"app_id": "a", "app_password": "p", "tenant_id": "t", "history_limit": 17},
    ))
    fake_graph = MagicMock()
    fake_graph.fetch_channel_messages = AsyncMock(return_value=[{"id": "m"}])
    adapter._graph = fake_graph

    result = await adapter.fetch_channel_history("t", "c")
    assert result == [{"id": "m"}]
    fake_graph.fetch_channel_messages.assert_awaited_once_with("t", "c", top=17)


@pytest.mark.asyncio
async def test_adapter_fetch_channel_history_returns_empty_without_graph():
    from gateway.platforms.msteams.adapter import MsTeamsAdapter
    from gateway.config import PlatformConfig

    adapter = MsTeamsAdapter(PlatformConfig(
        enabled=True,
        extra={"app_id": "a", "app_password": "p"},
    ))
    assert await adapter.fetch_channel_history("t", "c") == []


@pytest.mark.asyncio
async def test_adapter_upload_channel_file_requires_site_id():
    from gateway.platforms.msteams.adapter import MsTeamsAdapter
    from gateway.config import PlatformConfig

    adapter = MsTeamsAdapter(PlatformConfig(
        enabled=True,
        extra={"app_id": "a", "app_password": "p"},
    ))
    adapter._graph = MagicMock()  # Graph available but no site id configured
    result = await adapter.upload_channel_file("chat", "file.png", b"x")
    assert result is None


@pytest.mark.asyncio
async def test_adapter_upload_channel_file_namespaces_by_chat_id():
    from gateway.platforms.msteams.adapter import MsTeamsAdapter
    from gateway.config import PlatformConfig

    adapter = MsTeamsAdapter(PlatformConfig(
        enabled=True,
        extra={
            "app_id": "a", "app_password": "p",
            "sharepoint_site_id": "site-1", "sharepoint_folder": "Hermes",
        },
    ))
    fake_graph = MagicMock()
    fake_graph.upload_to_sharepoint = AsyncMock(return_value="https://sharepoint/x")
    adapter._graph = fake_graph

    url = await adapter.upload_channel_file("19:c@thread.tacv2", "doc.pdf", b"...")
    assert url == "https://sharepoint/x"

    call_kwargs = fake_graph.upload_to_sharepoint.call_args.kwargs
    assert call_kwargs["site_id"] == "site-1"
    assert call_kwargs["filename"] == "doc.pdf"
    # The chat id's colons / ats are sanitised so SharePoint accepts
    # the folder name.
    assert "Hermes/19_c_at_thread.tacv2" == call_kwargs["folder_path"]


# ---------------------------------------------------------------------------
# Adapter lifecycle hooks for Graph
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_adapter_builds_graph_client_on_connect(monkeypatch):
    from gateway.platforms.msteams.adapter import MsTeamsAdapter
    from gateway.config import PlatformConfig

    built: List[Any] = []

    real_init = GraphClient.__init__

    def _track_init(self, provider):
        real_init(self, provider)
        built.append(self)

    monkeypatch.setattr(GraphClient, "__init__", _track_init)

    # Fail out of connect() before aiohttp binds a port — but after the
    # Graph client is built — by feeding an empty host.
    adapter = MsTeamsAdapter(PlatformConfig(
        enabled=True,
        extra={
            "app_id": "a", "app_password": "p",
            "host": "invalid-host-that-cannot-bind",
            "port": 0,
        },
    ))

    # Stub out aiohttp bind so we don't actually open a socket.
    with patch("aiohttp.web.TCPSite.start", new=AsyncMock()):
        with patch.object(adapter, "_acquire_platform_lock", return_value=True):
            result = await adapter.connect()
    assert result is True
    assert len(built) == 1
    assert adapter._graph is built[0]
    await adapter.disconnect()
    assert adapter._graph is None
