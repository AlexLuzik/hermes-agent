"""Unit tests for ``gateway.platforms.msteams.auth`` (C2).

These tests exercise the credential-provider factory and the caching /
refresh behaviour of every provider variant.  The upstream MSAL and
azure-identity clients are patched at import time so the tests never
touch the network and run on any host — including developer laptops
that cannot talk to Azure Managed Identity.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import List

import pytest

from gateway.platforms.msteams import auth


# ---------------------------------------------------------------------------
# build_credential_provider — factory branching
# ---------------------------------------------------------------------------

def test_factory_defaults_to_secret_provider():
    prov = auth.build_credential_provider({
        "app_id": "app",
        "tenant_id": "tenant",
        "app_password": "s3cret",
    })
    assert isinstance(prov, auth.SecretCredentialProvider)
    assert prov.app_id == "app"
    assert prov.tenant_id == "tenant"


def test_factory_secret_requires_password():
    with pytest.raises(auth.AuthError, match="MSTEAMS_APP_PASSWORD"):
        auth.build_credential_provider({"app_id": "app", "tenant_id": "t"})


def test_factory_secret_requires_app_id():
    with pytest.raises(auth.AuthError, match="MSTEAMS_APP_ID"):
        auth.build_credential_provider({"app_password": "s", "tenant_id": "t"})


def test_factory_federated_cert(tmp_path):
    pem = tmp_path / "bot.pem"
    pem.write_text("-----BEGIN PRIVATE KEY-----\nX\n-----END PRIVATE KEY-----\n")
    prov = auth.build_credential_provider({
        "app_id": "app",
        "tenant_id": "tenant",
        "auth_type": "federated",
        "certificate_path": str(pem),
        "certificate_thumbprint": "AA:BB:CC",
    })
    assert isinstance(prov, auth.CertificateCredentialProvider)
    # Thumbprint normalized — colons removed, lowercased
    assert prov._cert_thumbprint == "aabbcc"


def test_factory_federated_managed_identity():
    prov = auth.build_credential_provider({
        "app_id": "app",
        "tenant_id": "tenant",
        "auth_type": "federated",
        "use_managed_identity": True,
        "managed_identity_client_id": "mi-1",
    })
    assert isinstance(prov, auth.ManagedIdentityCredentialProvider)
    assert prov._managed_identity_client_id == "mi-1"


def test_factory_federated_without_cert_or_mi_raises():
    with pytest.raises(auth.AuthError, match="federated"):
        auth.build_credential_provider({
            "app_id": "app",
            "tenant_id": "t",
            "auth_type": "federated",
        })


def test_factory_federated_cert_missing_thumbprint_raises(tmp_path):
    pem = tmp_path / "bot.pem"
    pem.write_text("x")
    with pytest.raises(auth.AuthError, match="THUMBPRINT"):
        auth.build_credential_provider({
            "app_id": "app",
            "tenant_id": "t",
            "auth_type": "federated",
            "certificate_path": str(pem),
        })


# ---------------------------------------------------------------------------
# _authority
# ---------------------------------------------------------------------------

def test_authority_uses_tenant():
    assert auth._authority("tid-123") == "https://login.microsoftonline.com/tid-123"


def test_authority_falls_back_to_common():
    assert auth._authority("") == "https://login.microsoftonline.com/common"
    assert auth._authority("   ") == "https://login.microsoftonline.com/common"


# ---------------------------------------------------------------------------
# SecretCredentialProvider — happy path + caching
# ---------------------------------------------------------------------------

class _FakeMsalApp:
    """Minimal stand-in for msal.ConfidentialClientApplication."""

    def __init__(self, tokens: List[dict]):
        self._tokens = list(tokens)
        self.calls: List[dict] = []

    def acquire_token_for_client(self, scopes):
        self.calls.append({"scopes": list(scopes)})
        if not self._tokens:
            return {"error": "exhausted", "error_description": "no more tokens"}
        return self._tokens.pop(0)


def _patch_msal_app(monkeypatch, provider, fake_app):
    monkeypatch.setattr(provider, "_build_msal_app", lambda: fake_app)


@pytest.mark.asyncio
async def test_secret_provider_happy_path(monkeypatch):
    prov = auth.SecretCredentialProvider("app", "tenant", "secret")
    fake = _FakeMsalApp([{"access_token": "tok1", "expires_in": 3600}])
    _patch_msal_app(monkeypatch, prov, fake)

    token = await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)
    assert token == "tok1"
    assert fake.calls == [{"scopes": [auth.BOT_FRAMEWORK_SCOPE]}]


@pytest.mark.asyncio
async def test_secret_provider_caches_within_lifetime(monkeypatch):
    prov = auth.SecretCredentialProvider("app", "tenant", "secret")
    fake = _FakeMsalApp([
        {"access_token": "tok1", "expires_in": 3600},
        {"access_token": "tok2", "expires_in": 3600},
    ])
    _patch_msal_app(monkeypatch, prov, fake)

    t1 = await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)
    t2 = await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)
    assert t1 == t2 == "tok1"
    assert len(fake.calls) == 1  # Second call served from cache


@pytest.mark.asyncio
async def test_secret_provider_refreshes_when_close_to_expiry(monkeypatch):
    prov = auth.SecretCredentialProvider("app", "tenant", "secret")
    fake = _FakeMsalApp([
        {"access_token": "old", "expires_in": 3600},
        {"access_token": "new", "expires_in": 3600},
    ])
    _patch_msal_app(monkeypatch, prov, fake)

    assert await prov.get_token(auth.BOT_FRAMEWORK_SCOPE) == "old"
    # Force cache entry into the refresh window.
    prov._cache[auth.BOT_FRAMEWORK_SCOPE].expires_at = (
        time.time() + auth._REFRESH_LEEWAY_SECONDS - 1
    )
    assert await prov.get_token(auth.BOT_FRAMEWORK_SCOPE) == "new"
    assert len(fake.calls) == 2


@pytest.mark.asyncio
async def test_secret_provider_separate_cache_per_scope(monkeypatch):
    prov = auth.SecretCredentialProvider("app", "tenant", "secret")
    fake = _FakeMsalApp([
        {"access_token": "bf-token", "expires_in": 3600},
        {"access_token": "gr-token", "expires_in": 3600},
    ])
    _patch_msal_app(monkeypatch, prov, fake)

    bf = await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)
    gr = await prov.get_token(auth.GRAPH_SCOPE)
    assert bf == "bf-token"
    assert gr == "gr-token"
    assert [c["scopes"][0] for c in fake.calls] == [
        auth.BOT_FRAMEWORK_SCOPE, auth.GRAPH_SCOPE,
    ]


@pytest.mark.asyncio
async def test_secret_provider_msal_error_raises_authError(monkeypatch):
    prov = auth.SecretCredentialProvider("app", "tenant", "secret")
    fake = _FakeMsalApp([{"error": "invalid_client", "error_description": "bad secret"}])
    _patch_msal_app(monkeypatch, prov, fake)

    with pytest.raises(auth.AuthError, match="bad secret"):
        await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)


@pytest.mark.asyncio
async def test_secret_provider_concurrent_requests_coalesce(monkeypatch):
    """Two concurrent get_token calls for the same scope must make only
    one MSAL request — the per-scope lock serializes the refresh and
    the second caller hits the cache populated by the first."""
    import threading
    prov = auth.SecretCredentialProvider("app", "tenant", "secret")

    call_count = 0
    proceed = threading.Event()

    class SlowFake:
        def acquire_token_for_client(self, scopes):
            nonlocal call_count
            # Hold the refresh on a plain threading.Event so the second
            # task has a chance to queue behind the per-scope asyncio.Lock.
            proceed.wait(timeout=5)
            call_count += 1
            return {"access_token": f"tok-{call_count}", "expires_in": 3600}

    _patch_msal_app(monkeypatch, prov, SlowFake())

    t1 = asyncio.create_task(prov.get_token(auth.BOT_FRAMEWORK_SCOPE))
    t2 = asyncio.create_task(prov.get_token(auth.BOT_FRAMEWORK_SCOPE))
    # Give both tasks a chance to reach the lock / be queued behind it.
    await asyncio.sleep(0.05)
    proceed.set()
    a, b = await asyncio.gather(t1, t2)
    assert a == b == "tok-1"
    assert call_count == 1


# ---------------------------------------------------------------------------
# CertificateCredentialProvider
# ---------------------------------------------------------------------------

class _FakeMsalCaptor:
    """Capture ConfidentialClientApplication construction args for inspection."""

    def __init__(self):
        self.kwargs = None

    def __call__(self, **kwargs):
        self.kwargs = kwargs

        class Dummy:
            def acquire_token_for_client(self_inner, scopes):
                return {"access_token": "cert-tok", "expires_in": 3600}

        return Dummy()


@pytest.mark.asyncio
async def test_certificate_provider_reads_pem_and_passes_thumbprint(
    monkeypatch, tmp_path,
):
    pem = tmp_path / "bot.pem"
    pem.write_text(
        "-----BEGIN PRIVATE KEY-----\nKEYDATA\n-----END PRIVATE KEY-----\n"
        "-----BEGIN CERTIFICATE-----\nCERTDATA\n-----END CERTIFICATE-----\n",
    )
    prov = auth.CertificateCredentialProvider(
        app_id="app",
        tenant_id="tenant",
        certificate_path=str(pem),
        certificate_thumbprint="AA:BB:cc",
    )
    captor = _FakeMsalCaptor()
    monkeypatch.setattr("msal.ConfidentialClientApplication", captor)

    tok = await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)
    assert tok == "cert-tok"
    assert captor.kwargs is not None
    assert captor.kwargs["client_id"] == "app"
    assert captor.kwargs["authority"] == "https://login.microsoftonline.com/tenant"
    credential = captor.kwargs["client_credential"]
    assert credential["thumbprint"] == "aabbcc"
    assert "KEYDATA" in credential["private_key"]
    assert "public_certificate" not in credential  # opt-in only


@pytest.mark.asyncio
async def test_certificate_provider_missing_file_raises(tmp_path):
    prov = auth.CertificateCredentialProvider(
        app_id="app",
        tenant_id="t",
        certificate_path=str(tmp_path / "missing.pem"),
        certificate_thumbprint="aa",
    )
    with pytest.raises(auth.AuthError, match="not found"):
        await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)


@pytest.mark.asyncio
async def test_certificate_provider_send_public_cert(monkeypatch, tmp_path):
    pem = tmp_path / "bot.pem"
    pem.write_text("PEM")
    prov = auth.CertificateCredentialProvider(
        app_id="app",
        tenant_id="t",
        certificate_path=str(pem),
        certificate_thumbprint="aa",
        send_public_cert=True,
    )
    captor = _FakeMsalCaptor()
    monkeypatch.setattr("msal.ConfidentialClientApplication", captor)
    await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)
    assert captor.kwargs["client_credential"]["public_certificate"] == "PEM"


# ---------------------------------------------------------------------------
# ManagedIdentityCredentialProvider
# ---------------------------------------------------------------------------

@dataclass
class _FakeAzureToken:
    token: str
    expires_on: int


class _FakeManagedIdentity:
    def __init__(self, client_id=None):
        self.client_id = client_id
        self.calls: List[str] = []

    def get_token(self, scope):
        self.calls.append(scope)
        return _FakeAzureToken(token=f"mi-{scope}", expires_on=int(time.time()) + 3600)


class _FailingManagedIdentity:
    def __init__(self, client_id=None):
        pass

    def get_token(self, scope):
        from azure.core.exceptions import ClientAuthenticationError
        raise ClientAuthenticationError("No identity endpoint")


@pytest.mark.asyncio
async def test_managed_identity_provider_happy_path(monkeypatch):
    fake = _FakeManagedIdentity(client_id="mi-1")
    monkeypatch.setattr(
        "azure.identity.ManagedIdentityCredential",
        lambda **kwargs: fake,
    )
    prov = auth.ManagedIdentityCredentialProvider(
        app_id="app", tenant_id="t", managed_identity_client_id="mi-1",
    )
    tok = await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)
    assert tok == f"mi-{auth.BOT_FRAMEWORK_SCOPE}"
    # Cached on second call
    await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)
    assert len(fake.calls) == 1


@pytest.mark.asyncio
async def test_managed_identity_provider_surface_auth_error(monkeypatch):
    monkeypatch.setattr(
        "azure.identity.ManagedIdentityCredential",
        lambda **kwargs: _FailingManagedIdentity(),
    )
    prov = auth.ManagedIdentityCredentialProvider(app_id="app", tenant_id="t")
    with pytest.raises(auth.AuthError, match="Managed Identity"):
        await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)


@pytest.mark.asyncio
async def test_managed_identity_provider_close_clears_cache(monkeypatch):
    fake = _FakeManagedIdentity()
    monkeypatch.setattr(
        "azure.identity.ManagedIdentityCredential", lambda **kwargs: fake,
    )
    prov = auth.ManagedIdentityCredentialProvider(app_id="a", tenant_id="t")
    await prov.get_token(auth.BOT_FRAMEWORK_SCOPE)
    assert prov._cache
    await prov.close()
    assert not prov._cache
    assert prov._credential is None
