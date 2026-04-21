"""Microsoft Teams platform adapter.

Bot Framework + Microsoft Graph integration for Hermes.  Full feature
parity with openclaw's Teams channel: DMs, channels, group chats,
Adaptive Cards, FileConsent uploads, SharePoint attachments, Graph-backed
history and @mention search.

The adapter runs its own aiohttp server for the Bot Framework webhook
(default port 3978, path /api/messages) and talks to the Microsoft Graph
API for features not exposed by the Bot Framework channel.
"""

from gateway.platforms.msteams.adapter import MsTeamsAdapter, check_msteams_requirements

__all__ = ["MsTeamsAdapter", "check_msteams_requirements"]
