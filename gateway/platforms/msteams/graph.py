"""Microsoft Graph client for the MS Teams adapter (C4).

Wraps ``msgraph.GraphServiceClient`` with a handful of task-specific
methods the adapter and senders call: channel history, member lookup,
user search, SharePoint uploads, and hosted-content downloads.  Each
method catches Graph SDK errors and returns an empty / ``None`` result
with a logged reason instead of propagating, so a Graph outage never
takes the Bot Framework event loop down with it.

Authentication goes through :class:`_HermesTokenCredential`, which
adapts our :mod:`.auth` :class:`CredentialProvider` to the
``azure.core.credentials_async.AsyncTokenCredential`` interface that the
Graph SDK and its Kiota request adapter expect.  That way the same
provider serves Bot Framework (scope ``BOT_FRAMEWORK_SCOPE``) and Graph
(``GRAPH_SCOPE``) calls through one cache.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from gateway.platforms.msteams.auth import (
    GRAPH_SCOPE,
    AuthError,
    CredentialProvider,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Credential adapter — bridges our async provider to azure.core.credentials_async
# ---------------------------------------------------------------------------

class _HermesTokenCredential:
    """Adapt :class:`CredentialProvider` to ``AsyncTokenCredential``.

    The Graph SDK calls ``await cred.get_token(*scopes, **kwargs)`` and
    expects a named tuple ``AccessToken(token, expires_on)``.  We delegate
    to the underlying provider and report an approximate ``expires_on``
    — our provider doesn't expose the real expiry to callers, and the
    Graph SDK only uses it as a best-effort refresh hint because it
    always requests a fresh token when it sees a 401.
    """

    def __init__(self, provider: CredentialProvider):
        self._provider = provider

    async def get_token(self, *scopes, **kwargs):
        from azure.core.credentials import AccessToken

        if not scopes:
            raise AuthError("AsyncTokenCredential.get_token requires at least one scope")
        token = await self._provider.get_token(scopes[0])
        return AccessToken(token, int(time.time()) + 3000)

    async def close(self):  # pragma: no cover - no-op
        return None


# ---------------------------------------------------------------------------
# Error-tolerant helpers
# ---------------------------------------------------------------------------

async def _safe(call, *, action: str, default: Any):
    """Run *call()* and return ``default`` on Graph errors after logging.

    Centralises error handling so every public method on
    :class:`GraphClient` stays short.  ``action`` is a free-form label
    that appears in log lines and makes Graph failures identifiable.
    """
    try:
        return await call()
    except Exception as exc:
        logger.warning("msteams.graph: %s failed: %s", action, exc)
        return default


def _attr(obj, name: str, default=None):
    """Attribute access that also tolerates dicts (useful in tests)."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


# ---------------------------------------------------------------------------
# GraphClient
# ---------------------------------------------------------------------------

class GraphClient:
    """Narrow façade over :class:`msgraph.GraphServiceClient`.

    The adapter constructs one instance per connect() cycle and shares
    it across inbound handlers and outbound senders.  The Graph SDK
    builds its HTTPS session lazily on the first call so a GraphClient
    without an actual network reachable endpoint is still cheap.
    """

    def __init__(self, provider: CredentialProvider):
        self._provider = provider
        self._client = None  # built lazily — msgraph-sdk pulls heavy kiota modules

    def _build_client(self):
        from msgraph import GraphServiceClient
        credential = _HermesTokenCredential(self._provider)
        return GraphServiceClient(
            credentials=credential,
            scopes=[GRAPH_SCOPE],
        )

    @property
    def client(self):
        if self._client is None:
            self._client = self._build_client()
        return self._client

    async def close(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        # msgraph-sdk's request_adapter owns the httpx client — close it
        # if we can, but don't sweat it when the SDK hides the plumbing.
        adapter = getattr(client, "request_adapter", None)
        http = getattr(adapter, "_http_client", None) if adapter else None
        close = getattr(http, "close", None) if http else None
        if callable(close):
            try:
                result = close()
                if hasattr(result, "__await__"):
                    await result
            except Exception:
                logger.debug("msteams.graph: close() raised", exc_info=True)

    # ------------------------------------------------------------------
    # Channel history
    # ------------------------------------------------------------------

    async def fetch_channel_messages(
        self, team_id: str, channel_id: str, top: int = 50,
    ) -> List[Dict[str, Any]]:
        """Return recent messages in a channel, oldest first.

        Powers the "prepend recent history to the first agent prompt in
        this channel" behaviour that Hermes already uses for Slack and
        Matrix.  Missing the ``ChannelMessage.Read.All`` permission or
        an RSC-scoped ``ChannelMessage.Read.Group`` returns an empty
        list — the caller downgrades gracefully rather than failing.
        """
        from msgraph.generated.teams.item.channels.item.messages.messages_request_builder import (
            MessagesRequestBuilder,
        )

        config = MessagesRequestBuilder.MessagesRequestBuilderGetRequestConfiguration(
            query_parameters=(
                MessagesRequestBuilder.MessagesRequestBuilderGetQueryParameters(
                    top=top,
                )
            ),
        )

        async def _call():
            page = await (
                self.client.teams.by_team_id(team_id)
                .channels.by_channel_id(channel_id)
                .messages
                .get(request_configuration=config)
            )
            if page is None or _attr(page, "value") is None:
                return []
            return [_message_to_dict(m) for m in _attr(page, "value") or []][::-1]

        return await _safe(_call, action=f"fetch_channel_messages({channel_id})", default=[])

    # ------------------------------------------------------------------
    # Users
    # ------------------------------------------------------------------

    async def resolve_user(self, aad_object_id: str) -> Optional[Dict[str, Any]]:
        """Look up the display name, email, and role of an AAD user."""

        async def _call():
            user = await self.client.users.by_user_id(aad_object_id).get()
            if user is None:
                return None
            return {
                "id": _attr(user, "id"),
                "display_name": _attr(user, "display_name"),
                "email": _attr(user, "mail") or _attr(user, "user_principal_name"),
                "job_title": _attr(user, "job_title"),
            }

        return await _safe(_call, action=f"resolve_user({aad_object_id})", default=None)

    async def search_users_by_display_name(
        self, prefix: str, top: int = 5,
    ) -> List[Dict[str, Any]]:
        """Lookup users whose displayName starts with *prefix*.

        Used by the agent when it wants to turn a human name in its
        response ("@John") into a real @mention.  Returns an empty list
        if the tenant hasn't granted ``User.Read.All``.
        """
        from msgraph.generated.users.users_request_builder import UsersRequestBuilder

        safe_prefix = prefix.replace("'", "''")
        config = UsersRequestBuilder.UsersRequestBuilderGetRequestConfiguration(
            query_parameters=UsersRequestBuilder.UsersRequestBuilderGetQueryParameters(
                filter=f"startswith(displayName,'{safe_prefix}')",
                top=top,
                select=["id", "displayName", "mail", "userPrincipalName"],
            ),
        )

        async def _call():
            page = await self.client.users.get(request_configuration=config)
            if page is None or _attr(page, "value") is None:
                return []
            results = []
            for u in _attr(page, "value") or []:
                results.append({
                    "id": _attr(u, "id"),
                    "display_name": _attr(u, "display_name"),
                    "email": _attr(u, "mail") or _attr(u, "user_principal_name"),
                })
            return results

        return await _safe(
            _call, action=f"search_users_by_display_name({prefix!r})", default=[],
        )

    # ------------------------------------------------------------------
    # Teams + channels enumeration (for the channel directory)
    # ------------------------------------------------------------------

    async def list_joined_teams(self) -> List[Dict[str, Any]]:
        async def _call():
            page = await self.client.me.joined_teams.get()
            if page is None or _attr(page, "value") is None:
                return []
            return [
                {
                    "id": _attr(t, "id"),
                    "display_name": _attr(t, "display_name"),
                    "description": _attr(t, "description"),
                }
                for t in _attr(page, "value") or []
            ]

        return await _safe(_call, action="list_joined_teams", default=[])

    async def list_channels(self, team_id: str) -> List[Dict[str, Any]]:
        async def _call():
            page = await self.client.teams.by_team_id(team_id).channels.get()
            if page is None or _attr(page, "value") is None:
                return []
            return [
                {
                    "id": _attr(c, "id"),
                    "display_name": _attr(c, "display_name"),
                    "description": _attr(c, "description"),
                    "membership_type": str(_attr(c, "membership_type") or ""),
                }
                for c in _attr(page, "value") or []
            ]

        return await _safe(_call, action=f"list_channels({team_id})", default=[])

    # ------------------------------------------------------------------
    # Hosted content (inline attachments inside channel messages)
    # ------------------------------------------------------------------

    async def download_hosted_content(
        self,
        team_id: str, channel_id: str,
        message_id: str, hosted_content_id: str,
    ) -> Optional[bytes]:
        """Fetch the raw bytes of an inline image / attachment in a channel
        message.  Returns ``None`` if the call fails — the adapter logs
        the miss and passes only the URL reference through to the agent.
        """

        async def _call():
            return await (
                self.client.teams.by_team_id(team_id)
                .channels.by_channel_id(channel_id)
                .messages.by_chat_message_id(message_id)
                .hosted_contents.by_chat_message_hosted_content_id(hosted_content_id)
                .content.get()
            )

        return await _safe(
            _call,
            action=f"download_hosted_content({message_id}/{hosted_content_id})",
            default=None,
        )

    # ------------------------------------------------------------------
    # SharePoint upload (channel / group file attachments)
    # ------------------------------------------------------------------

    async def upload_to_sharepoint(
        self,
        site_id: str,
        folder_path: str,
        filename: str,
        content: bytes,
    ) -> Optional[str]:
        """Upload *content* to a SharePoint document library folder.

        Returns the ``webUrl`` the adapter can attach to a Teams message
        so recipients see a proper file card.  Graph's simple-upload
        endpoint (PUT /content) caps at 4 MB per request — callers with
        larger payloads should chunk via createUploadSession, but that
        path is out of scope for C4's MVP.
        """
        clean_folder = folder_path.strip("/")
        path_prefix = f"/{clean_folder}/" if clean_folder else "/"
        # Graph drive item paths use colon-delimited "path" syntax:
        # /sites/{site}/drive/root:/folder/file.png:/content
        _encoded_path = f"{path_prefix}{filename}"

        async def _call():
            from msgraph.generated.drives.item.items.item.content.content_request_builder import (
                ContentRequestBuilder,  # noqa: F401 — import validates SDK shape
            )
            drive = self.client.sites.by_site_id(site_id).drive
            item = drive.items.by_drive_item_id(f"root:{_encoded_path}:")
            result = await item.content.put(content)
            return _attr(result, "web_url") or _attr(result, "_raw_url")

        return await _safe(
            _call,
            action=f"upload_to_sharepoint({site_id}/{filename})",
            default=None,
        )


# ---------------------------------------------------------------------------
# Message flattening
# ---------------------------------------------------------------------------

def _message_to_dict(message) -> Dict[str, Any]:
    """Reduce a ``chatMessage`` to the fields the agent actually reads.

    Kept public so the adapter can cheaply project messages fetched
    through other Graph endpoints (e.g. ``/chats/{id}/messages``) into
    the same shape used by :meth:`GraphClient.fetch_channel_messages`.
    """
    body = _attr(message, "body")
    from_ = _attr(message, "from_")  # SDK renames "from" to avoid the keyword
    if from_ is None:
        from_ = _attr(message, "from")
    from_user = _attr(from_, "user") if from_ else None
    return {
        "id": _attr(message, "id"),
        "created_date_time": str(_attr(message, "created_date_time") or ""),
        "text": _attr(body, "content") or "",
        "content_type": str(_attr(body, "content_type") or "text"),
        "from_id": _attr(from_user, "id") if from_user else None,
        "from_name": _attr(from_user, "display_name") if from_user else None,
        "reply_to_id": _attr(message, "reply_to_id"),
    }
