"""Microsoft Teams Bot Framework adapter (C1 scaffolding).

This file holds the ``MsTeamsAdapter`` class.  In C1 the adapter only
implements the wiring so that the rest of the gateway code can import,
construct, and route to it.  The real Bot Framework + Graph protocol
lands in C2–C5 (see the msteams implementation plan).

Design notes:
- The adapter runs its own aiohttp HTTP server (default port 3978,
  path /api/messages) — it does NOT share the api_server / webhook
  listeners.  This matches the openclaw MS Teams shape and the existing
  Hermes pattern (each incoming-HTTP adapter owns its own aiohttp app).
- Authentication and Graph integration are deliberately split into
  ``auth.py`` and ``graph.py`` so they can be unit-tested without the
  Bot Framework SDK mocked in.  ``cards.py`` holds Adaptive Card and
  FileConsent card builders plus the markdown→Teams-HTML converter.
"""

from __future__ import annotations

import logging

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult

logger = logging.getLogger(__name__)


def check_msteams_requirements() -> bool:
    """Return True iff every MS Teams runtime dependency imports cleanly.

    The real adapter needs botbuilder-core (activity parsing),
    botframework-connector (JWT validation + outbound auth), msal +
    azure-identity (secret / certificate / Managed Identity auth),
    msgraph-sdk (Graph features), and aiohttp (webhook server).  We
    soft-import all of them here so the gateway can log a clear error
    and skip the adapter when the ``[msteams]`` extra is not installed.
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


class MsTeamsAdapter(BasePlatformAdapter):
    """Stub adapter — connects to nothing yet.

    Replaced by the real implementation in C3+.  Keeping the stub here
    lets the gateway factory, authorization maps, send_message tool, and
    cron scheduler all reference ``Platform.MSTEAMS`` end-to-end in C1
    without breaking anything at runtime.
    """

    MAX_MESSAGE_LENGTH = 28000  # Teams activity text budget

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.MSTEAMS)
        self.gateway_runner = None  # set by run.py for cross-platform delivery
        extra = config.extra or {}
        self._host: str = str(extra.get("host") or "0.0.0.0")
        self._port: int = int(extra.get("port") or 3978)
        self._path: str = str(extra.get("path") or "/api/messages")
        self._app_id: str = str(extra.get("app_id") or "")
        self._tenant_id: str = str(extra.get("tenant_id") or "")
        self._auth_type: str = str(extra.get("auth_type") or "secret").lower()
        self._require_mention: bool = bool(extra.get("require_mention", True))
        self._reply_style: str = str(extra.get("reply_style") or "thread")
        self._dm_policy: str = str(extra.get("dm_policy") or "pairing")
        self._allow_from: list = list(extra.get("allow_from") or [])
        self._group_allow_from: list = list(extra.get("group_allow_from") or [])
        self._sharepoint_site_id: str = str(extra.get("sharepoint_site_id") or "")
        self._history_limit: int = int(extra.get("history_limit") or 50)
        self._free_response_channels: list = list(extra.get("free_response_channels") or [])

    @property
    def name(self) -> str:
        return "msteams"

    async def connect(self) -> bool:
        logger.warning(
            "MSTeams adapter is a C1 stub — protocol implementation lands in "
            "commits C2–C5.  The adapter will not connect yet.",
        )
        return False

    async def disconnect(self) -> None:
        return None

    async def send(
        self,
        chat_id,
        content,
        reply_to=None,
        metadata=None,
    ) -> SendResult:
        return SendResult(
            success=False,
            error="MSTeams send not yet implemented (C1 stub)",
            retryable=False,
        )

    async def send_typing(self, chat_id, metadata=None):
        return None

    async def send_image(
        self,
        chat_id,
        image_url: str,
        caption: str = "",
        reply_to=None,
        metadata=None,
    ) -> SendResult:
        return SendResult(
            success=False,
            error="MSTeams send_image not yet implemented (C1 stub)",
            retryable=False,
        )

    async def get_chat_info(self, chat_id) -> dict:
        return {"name": str(chat_id), "type": "unknown", "chat_id": str(chat_id)}
