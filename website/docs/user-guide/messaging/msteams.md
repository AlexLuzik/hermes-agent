---
sidebar_position: 9
title: "Microsoft Teams"
description: "Set up Hermes Agent as a Microsoft Teams bot with Bot Framework and Graph"
---

# Microsoft Teams Setup

Hermes integrates with Microsoft Teams through the Bot Framework v3 channel and Microsoft Graph. The adapter receives activities on its own HTTPS webhook (default port 3978, path `/api/messages`), validates the JWT Microsoft signs into every inbound request, and replies by POSTing back to the `serviceUrl` the activity reports. Channel uploads flow through SharePoint via Graph; DM uploads use the FileConsent card flow.

Install the Teams extras once before starting the gateway:

```bash
pip install 'hermes-agent[msteams]'
```

This pulls `botbuilder-core`, `botframework-connector`, `msal`, `azure-identity`, and `msgraph-sdk`.

## How Hermes Behaves

| Context | Behavior |
|---------|----------|
| **1:1 chat (DM)** | Hermes responds to every message from allowlisted AAD users. The `dm_policy` knob selects the gate: `pairing` (default — Hermes' session-pairing flow), `allowlist` (strict `MSTEAMS_ALLOW_FROM`), `open` (any user the Teams app is installed for), `disabled`. |
| **Channels** | Hermes only replies to posts that `@mention` the bot, unless the conversation is listed in `MSTEAMS_FREE_RESPONSE_CHANNELS`. Conversations are session-partitioned per channel thread. |
| **Group chats** | Same as channels, gated by `MSTEAMS_GROUP_ALLOW_FROM`. |
| **Files** | DMs trigger a FileConsent card — the user confirms, Hermes uploads into the user's OneDrive. Channels upload to a configured SharePoint site and post a Teams file card. |
| **Adaptive Cards** | Hermes can emit Adaptive Card attachments; incoming card-action `invoke` activities are accepted. |

## Overview

1. Register an Azure AD app and mint credentials (secret *or* certificate *or* Managed Identity).
2. Create a Bot Framework resource that points at Hermes' webhook.
3. Upload a Teams app manifest to Teams admin center and install it.
4. Configure `~/.hermes/.env` and start `hermes gateway`.

## Step 1 — Azure App Registration

1. Open Azure Portal → **App registrations** → **New registration**.
2. Give the app a name (e.g. `hermes-agent`), accept defaults, and create it.
3. Copy the **Application (client) ID** — this is `MSTEAMS_APP_ID`.
4. Copy the **Directory (tenant) ID** — this is `MSTEAMS_TENANT_ID`. Use `common` if you want the bot to work across tenants.

### Option A — Client Secret (simplest)

5. Open **Certificates & secrets** → **New client secret**. Set the lifetime you want and copy the **Value** (not the Secret ID). This is `MSTEAMS_APP_PASSWORD`.

### Option B — Certificate (federated auth)

5. Generate a self-signed cert or obtain one from your internal PKI. Upload the public part under **Certificates & secrets → Certificates** and copy the thumbprint.
6. Point `MSTEAMS_CERTIFICATE_PATH` at a PEM file containing both the private key and the certificate body. Set `MSTEAMS_CERTIFICATE_THUMBPRINT` and `MSTEAMS_AUTH_TYPE=federated`.

### Option C — Azure Managed Identity (Azure-hosted only)

5. Assign a system- or user-assigned Managed Identity to the Azure compute running Hermes (App Service, AKS, Container Apps, Functions).
6. In **App registrations → Owners → Federated credentials**, add the Managed Identity as a federated credential issuer.
7. Set `MSTEAMS_USE_MANAGED_IDENTITY=true`. For user-assigned identities, also set `MSTEAMS_MANAGED_IDENTITY_CLIENT_ID`.

:::warning
Managed Identity only works when the process runs on Azure compute. On a developer laptop `azure.identity.ManagedIdentityCredential` returns `CredentialUnavailableError`. Use Option A or B locally.
:::

## Step 2 — API Permissions

Under **API permissions**, add the following to the app registration. All are **Application** permissions (Graph admin consent required):

| Permission | Purpose |
|---|---|
| `User.Read.All` | Display name / email lookup, `@mention` expansion |
| `ChannelMessage.Read.All` | Channel history backfill |
| `Sites.ReadWrite.All` | SharePoint uploads for channel file cards |

Click **Grant admin consent** after saving.

Bot Framework messaging permissions live on the Teams side and are delivered via the app manifest (Step 4 — RSC).

## Step 3 — Bot Framework Resource

1. Go to [Bot Framework Portal](https://dev.botframework.com/bots) → **Create a Bot**.
2. Select **Use existing app registration** and paste the Application (client) ID from Step 1.
3. Set **Messaging endpoint** to `https://<your-public-host>:3978/api/messages`. For local testing, expose Hermes with [ngrok](https://ngrok.com) or Cloudflare Tunnel and use the assigned HTTPS URL.
4. Under **Channels**, add **Microsoft Teams**.

## Step 4 — Teams App Manifest

Create a `manifest.json` next to a 32×32 outline icon and a 192×192 color icon, then zip all three as `hermes.zip`. Minimum contents:

```json
{
  "$schema": "https://developer.microsoft.com/en-us/json-schemas/teams/v1.17/MicrosoftTeams.schema.json",
  "manifestVersion": "1.17",
  "version": "1.0.0",
  "id": "<MSTEAMS_APP_ID>",
  "packageName": "com.example.hermes",
  "developer": {
    "name": "Hermes",
    "websiteUrl": "https://hermes-agent.nousresearch.com",
    "privacyUrl": "https://hermes-agent.nousresearch.com/privacy",
    "termsOfUseUrl": "https://hermes-agent.nousresearch.com/terms"
  },
  "name": { "short": "Hermes", "full": "Hermes Agent" },
  "description": {
    "short": "Hermes autonomous agent",
    "full": "Hermes Agent — an autonomous agent that gets more capable the longer it runs."
  },
  "icons": { "color": "color.png", "outline": "outline.png" },
  "accentColor": "#1B6EC2",
  "bots": [{
    "botId": "<MSTEAMS_APP_ID>",
    "scopes": ["personal", "team", "groupChat"],
    "supportsFiles": true,
    "isNotificationOnly": false
  }],
  "permissions": ["identity", "messageTeamMembers"],
  "validDomains": ["*.sharepoint.com", "smba.trafficmanager.net"],
  "authorization": {
    "permissions": {
      "resourceSpecific": [
        { "name": "ChannelMessage.Read.Group", "type": "Application" },
        { "name": "ChannelMessage.Send.Group", "type": "Application" },
        { "name": "ChatMessage.Read.Chat",     "type": "Application" },
        { "name": "TeamSettings.Read.Group",   "type": "Application" },
        { "name": "ChannelSettings.Read.Group","type": "Application" }
      ]
    }
  }
}
```

Upload the zip in **Teams Admin Center → Teams apps → Manage apps → Upload** and approve it for your tenant.

## Step 5 — Configure Hermes

Add the following to `~/.hermes/.env`. Only the first three lines are always required; everything else has a working default.

```bash
MSTEAMS_APP_ID=00000000-0000-0000-0000-000000000000
MSTEAMS_TENANT_ID=11111111-1111-1111-1111-111111111111
MSTEAMS_APP_PASSWORD=<client secret from step 1>

# Or, for federated auth:
# MSTEAMS_AUTH_TYPE=federated
# MSTEAMS_CERTIFICATE_PATH=/etc/hermes/teams-bot.pem
# MSTEAMS_CERTIFICATE_THUMBPRINT=AABBCC...
# MSTEAMS_USE_MANAGED_IDENTITY=true

MSTEAMS_HOST=0.0.0.0
MSTEAMS_PORT=3978
MSTEAMS_PATH=/api/messages

# Who can reach the bot
MSTEAMS_DM_POLICY=pairing
MSTEAMS_ALLOWED_USERS=<comma-separated AAD Object IDs>
MSTEAMS_GROUP_ALLOW_FROM=<comma-separated AAD Object IDs for channels/groups>

# Channel behavior
MSTEAMS_REQUIRE_MENTION=true
MSTEAMS_REPLY_STYLE=thread
MSTEAMS_HISTORY_LIMIT=50
MSTEAMS_FREE_RESPONSE_CHANNELS=<optional comma-separated channel ids>

# SharePoint (required for channel / group file uploads)
MSTEAMS_SHAREPOINT_SITE_ID=<site id or hostname,site-path>

# Optional home channel for cron delivery
MSTEAMS_HOME_CHANNEL=<conversation id>
MSTEAMS_HOME_CHANNEL_NAME=Teams Home
```

Start the gateway:

```bash
hermes gateway
```

DM the bot in Teams or `@mention` it in a channel. The first message records the conversation's `serviceUrl` under `~/.hermes/msteams/service_urls.json` so the out-of-process `send_message` tool and cron jobs can reach the same chat later.

## Per-team / per-channel Overrides

`~/.hermes/cli-config.yaml`:

```yaml
platforms:
  msteams:
    extra:
      teams:
        "<team-aad-group-id>":
          require_mention: false
          reply_style: top-level
          channels:
            "19:ch@thread.tacv2":
              require_mention: true
              allow_from: ["aad-admin"]
```

Channel-level overrides take precedence over team-level, which takes precedence over the adapter defaults. Unset fields inherit.

## Troubleshooting

### Bot never responds to messages

Most common cause: the messaging endpoint is not publicly reachable. Teams requires a valid HTTPS URL with a certificate signed by a public CA. Double-check with:

```bash
curl -i https://<your-host>:3978/health
```

It should return `{"platform":"msteams",...}`. If it doesn't, your firewall / tunnel is blocking the port.

### "JWT validation failed"

The Azure App ID in `MSTEAMS_APP_ID` doesn't match the bot that sent the activity. Check the **Microsoft App ID** field of the Bot Framework resource (Step 3) and make sure it equals the value in your env file.

### "SHAREPOINT_SITE_ID is not configured"

Channel / group file uploads need a SharePoint site the bot can write to. Create one under **SharePoint → Sites**, grant the app the `Sites.ReadWrite.All` Graph permission, and set `MSTEAMS_SHAREPOINT_SITE_ID` to the site id (visible in the site URL or via `GET /sites/{hostname}:/sites/{path}`).

### FileConsent upload never completes

The FileConsent flow is DM-only and relies on the user clicking **Allow**. If the follow-up `FileInfoCard` never appears, check:

- Hermes log for `fileConsent upload failed` — usually a stale upload URL (Teams retries the invoke). Ask the user to trigger the send again.
- The bot is missing the `supportsFiles: true` entry in the app manifest.

### Managed Identity "CredentialUnavailableError"

Managed Identity only works on Azure compute. Switch to client secret or certificate auth for local dev.

## Security

:::warning
Always set `MSTEAMS_ALLOWED_USERS` (or `MSTEAMS_DM_POLICY=allowlist`) and `MSTEAMS_GROUP_ALLOW_FROM` before exposing the bot. Without those gates, any user the Teams app is installed for can trigger agent runs — including running tools, spending LLM tokens, and triggering system-update commands.
:::

For more information see the [Security Guide](../security.md).

## Notes

- The adapter does not use the Bot Framework Emulator adapter; it only depends on `JwtTokenValidation` for inbound JWT checks plus raw `botbuilder.schema.Activity` for deserialisation. Outbound calls use our own `aiohttp.ClientSession` so the Hermes event loop stays in charge.
- Federated auth validates the same way as secret auth — inbound tokens are signed by Microsoft's public JWKS, independent of how outbound tokens are minted.
- Channel messages arrive with a SharePoint reference for attachments; the adapter uses Graph to fetch the raw bytes when needed.
