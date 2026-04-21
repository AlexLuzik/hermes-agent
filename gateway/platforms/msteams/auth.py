"""Microsoft identity providers for the MS Teams adapter.

Three auth flows are supported, chosen by ``config.extra['auth_type']``:

- ``secret`` (default) — client secret via MSAL ``ConfidentialClientApplication``.
- ``federated`` with ``certificate_path`` — X.509 certificate flow via MSAL.
- ``federated`` with ``use_managed_identity=true`` — Azure Managed Identity
  via ``azure.identity.ManagedIdentityCredential`` (only usable on Azure
  compute such as App Service, AKS, Container Apps, Functions).

All providers expose the same async ``get_token(scope)`` surface.  The
adapter and Graph client call it once per outbound request; the provider
caches per-scope tokens and refreshes 5 minutes before expiry so the
common case is a dict lookup.

MSAL itself is synchronous.  We run its blocking calls in a thread pool
via ``asyncio.to_thread`` so the Hermes gateway's event loop stays free.
Each scope has its own ``asyncio.Lock`` so concurrent token requests for
the same scope coalesce into a single refresh.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Protocol

logger = logging.getLogger(__name__)

# Standard scopes.  Importers prefer these constants over string literals
# so a typo fails at import time rather than at first request.
BOT_FRAMEWORK_SCOPE = "https://api.botframework.com/.default"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

# Refresh tokens this many seconds before their expiry.  MSAL returns
# ``expires_in`` in seconds; Azure typically issues tokens with a 3600s
# lifetime, so 300s of headroom is comfortable while still letting the
# cache hit the vast majority of requests.
_REFRESH_LEEWAY_SECONDS = 300


class AuthError(Exception):
    """Raised when a token cannot be acquired.

    Carries the MSAL / Azure ``error_description`` so log messages point
    directly at the failing credential rather than surfacing as a generic
    aiohttp error later in the stack.
    """


@dataclass
class _CachedToken:
    access_token: str
    expires_at: float  # Unix timestamp


class CredentialProvider(Protocol):
    """Minimal async interface every auth strategy implements."""

    app_id: str
    tenant_id: str

    async def get_token(self, scope: str) -> str:  # pragma: no cover - Protocol
        ...

    async def close(self) -> None:  # pragma: no cover - Protocol
        ...


def _authority(tenant_id: str) -> str:
    """Return the MSAL authority URL for a tenant.

    Multi-tenant bots configured with ``tenant_id="common"`` or empty
    still need a valid URL; MSAL accepts ``login.microsoftonline.com/common``
    and the Bot Framework backend handles it via its own trust configuration.
    """
    tenant = tenant_id.strip() or "common"
    return f"https://login.microsoftonline.com/{tenant}"


class _MsalBackedProvider:
    """Shared token cache + MSAL plumbing for secret and certificate flows.

    The cache is keyed on scope so the same provider services both Bot
    Framework and Graph calls.  MSAL's own in-memory cache would be enough
    for a single-process run, but our wrapper adds ``asyncio.Lock`` per
    scope so concurrent ``get_token`` calls do not trigger duplicate
    network round trips.
    """

    def __init__(self, app_id: str, tenant_id: str):
        self.app_id = app_id
        self.tenant_id = tenant_id
        self._msal_app = None  # Lazily built in subclass
        self._cache: Dict[str, _CachedToken] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _get_lock(self, scope: str) -> asyncio.Lock:
        lock = self._locks.get(scope)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[scope] = lock
        return lock

    def _build_msal_app(self):  # pragma: no cover - overridden
        raise NotImplementedError

    async def get_token(self, scope: str) -> str:
        now = time.time()
        cached = self._cache.get(scope)
        if cached and cached.expires_at - _REFRESH_LEEWAY_SECONDS > now:
            return cached.access_token

        async with self._get_lock(scope):
            # Re-check after winning the lock — another coroutine may have
            # refreshed while we waited.
            cached = self._cache.get(scope)
            now = time.time()
            if cached and cached.expires_at - _REFRESH_LEEWAY_SECONDS > now:
                return cached.access_token

            if self._msal_app is None:
                self._msal_app = await asyncio.to_thread(self._build_msal_app)

            result = await asyncio.to_thread(
                self._msal_app.acquire_token_for_client,
                scopes=[scope],
            )
            if "access_token" not in result:
                err = result.get("error_description") or result.get("error") or "unknown"
                raise AuthError(
                    f"MSAL token acquisition failed for scope {scope}: {err}",
                )
            expires_in = int(result.get("expires_in", 3600))
            token = _CachedToken(
                access_token=result["access_token"],
                expires_at=time.time() + expires_in,
            )
            self._cache[scope] = token
            logger.debug(
                "msteams.auth: acquired token for scope=%s (lifetime=%ds)",
                scope, expires_in,
            )
            return token.access_token

    async def close(self) -> None:
        self._cache.clear()


class SecretCredentialProvider(_MsalBackedProvider):
    """Client-secret flow (``MSTEAMS_AUTH_TYPE=secret``)."""

    def __init__(self, app_id: str, tenant_id: str, app_password: str):
        if not app_id:
            raise AuthError("MSTEAMS_APP_ID is required for secret auth")
        if not app_password:
            raise AuthError("MSTEAMS_APP_PASSWORD is required for secret auth")
        super().__init__(app_id=app_id, tenant_id=tenant_id)
        self._app_password = app_password

    def _build_msal_app(self):
        from msal import ConfidentialClientApplication
        return ConfidentialClientApplication(
            client_id=self.app_id,
            authority=_authority(self.tenant_id),
            client_credential=self._app_password,
        )


class CertificateCredentialProvider(_MsalBackedProvider):
    """Certificate flow for federated auth.

    Reads a PEM file that contains both the private key and the X.509
    certificate body.  The SHA-1 thumbprint must match what you registered
    in the Azure app registration (Certificates & secrets → Certificates).
    MSAL can optionally send the whole public-cert chain with each request
    to enable subject-name/issuer trust; that is opt-in via
    ``MSTEAMS_CERTIFICATE_SEND_PUBLIC``.
    """

    def __init__(
        self,
        app_id: str,
        tenant_id: str,
        certificate_path: str,
        certificate_thumbprint: str,
        send_public_cert: bool = False,
    ):
        if not app_id:
            raise AuthError("MSTEAMS_APP_ID is required for federated auth")
        if not certificate_path:
            raise AuthError("MSTEAMS_CERTIFICATE_PATH is required for federated auth")
        if not certificate_thumbprint:
            raise AuthError(
                "MSTEAMS_CERTIFICATE_THUMBPRINT is required for federated auth",
            )
        super().__init__(app_id=app_id, tenant_id=tenant_id)
        self._cert_path = Path(certificate_path).expanduser()
        self._cert_thumbprint = certificate_thumbprint.replace(":", "").strip().lower()
        self._send_public_cert = send_public_cert

    def _build_msal_app(self):
        from msal import ConfidentialClientApplication

        if not self._cert_path.is_file():
            raise AuthError(
                f"MSTEAMS certificate not found at {self._cert_path}",
            )
        pem = self._cert_path.read_text(encoding="utf-8")
        client_credential: Dict[str, Any] = {
            "private_key": pem,
            "thumbprint": self._cert_thumbprint,
        }
        if self._send_public_cert:
            client_credential["public_certificate"] = pem
        return ConfidentialClientApplication(
            client_id=self.app_id,
            authority=_authority(self.tenant_id),
            client_credential=client_credential,
        )


class ManagedIdentityCredentialProvider:
    """Azure Managed Identity flow (only usable on Azure compute).

    On non-Azure hosts ``azure.identity.ManagedIdentityCredential`` raises
    ``CredentialUnavailableError`` on the first ``get_token`` call; that
    surfaces as :class:`AuthError` here so the adapter's startup log makes
    the root cause obvious.
    """

    def __init__(self, app_id: str, tenant_id: str, managed_identity_client_id: str = ""):
        self.app_id = app_id
        self.tenant_id = tenant_id
        self._managed_identity_client_id = managed_identity_client_id.strip()
        self._credential = None
        self._cache: Dict[str, _CachedToken] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _get_lock(self, scope: str) -> asyncio.Lock:
        lock = self._locks.get(scope)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[scope] = lock
        return lock

    def _build_credential(self):
        from azure.identity import ManagedIdentityCredential
        if self._managed_identity_client_id:
            return ManagedIdentityCredential(
                client_id=self._managed_identity_client_id,
            )
        return ManagedIdentityCredential()

    async def get_token(self, scope: str) -> str:
        now = time.time()
        cached = self._cache.get(scope)
        if cached and cached.expires_at - _REFRESH_LEEWAY_SECONDS > now:
            return cached.access_token

        async with self._get_lock(scope):
            cached = self._cache.get(scope)
            now = time.time()
            if cached and cached.expires_at - _REFRESH_LEEWAY_SECONDS > now:
                return cached.access_token

            if self._credential is None:
                self._credential = await asyncio.to_thread(self._build_credential)

            try:
                # azure.identity returns ``AccessToken(token, expires_on)`` —
                # expires_on is a Unix timestamp, not a relative seconds-until.
                token_result = await asyncio.to_thread(
                    self._credential.get_token, scope,
                )
            except Exception as exc:  # CredentialUnavailableError, ClientAuthenticationError, ...
                raise AuthError(
                    f"Managed Identity token acquisition failed for scope {scope}: {exc}",
                ) from exc

            token = _CachedToken(
                access_token=token_result.token,
                expires_at=float(token_result.expires_on),
            )
            self._cache[scope] = token
            logger.debug(
                "msteams.auth: acquired MI token for scope=%s (expires_on=%.0f)",
                scope, token.expires_at,
            )
            return token.access_token

    async def close(self) -> None:
        cred = self._credential
        if cred is not None:
            close = getattr(cred, "close", None)
            if close is not None:
                try:
                    result = close()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    logger.debug("msteams.auth: ManagedIdentityCredential.close raised", exc_info=True)
        self._credential = None
        self._cache.clear()


def build_credential_provider(extra: Optional[Dict[str, Any]]) -> CredentialProvider:
    """Construct the right provider for ``PlatformConfig.extra``.

    Selection rules:
      - ``auth_type=federated`` + ``use_managed_identity=true`` →
        :class:`ManagedIdentityCredentialProvider`
      - ``auth_type=federated`` + ``certificate_path`` →
        :class:`CertificateCredentialProvider`
      - anything else (default) → :class:`SecretCredentialProvider`

    Invalid combinations (federated without cert and without MI, missing
    app_id, missing secret in the secret flow) raise :class:`AuthError`
    so the gateway skips the adapter with a clear log line.
    """
    extra = dict(extra or {})
    auth_type = str(extra.get("auth_type") or "secret").lower()
    app_id = str(extra.get("app_id") or "").strip()
    tenant_id = str(extra.get("tenant_id") or "").strip()

    if auth_type == "federated":
        use_mi = bool(extra.get("use_managed_identity"))
        cert_path = str(extra.get("certificate_path") or "").strip()
        cert_thumbprint = str(extra.get("certificate_thumbprint") or "").strip()
        if use_mi:
            return ManagedIdentityCredentialProvider(
                app_id=app_id,
                tenant_id=tenant_id,
                managed_identity_client_id=str(
                    extra.get("managed_identity_client_id") or "",
                ),
            )
        if cert_path:
            return CertificateCredentialProvider(
                app_id=app_id,
                tenant_id=tenant_id,
                certificate_path=cert_path,
                certificate_thumbprint=cert_thumbprint,
                send_public_cert=bool(extra.get("certificate_send_public")),
            )
        raise AuthError(
            "auth_type=federated requires either MSTEAMS_USE_MANAGED_IDENTITY=true "
            "or MSTEAMS_CERTIFICATE_PATH + MSTEAMS_CERTIFICATE_THUMBPRINT",
        )

    # Default: client secret
    return SecretCredentialProvider(
        app_id=app_id,
        tenant_id=tenant_id,
        app_password=str(extra.get("app_password") or ""),
    )
