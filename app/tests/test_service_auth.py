from __future__ import annotations

import time

import pytest
from joserfc import jwt
from joserfc.jwk import KeySet, RSAKey

from app.application.errors.exceptions import ForbiddenError, UnauthorizedError
from app.infrastructure.security.oidc import OidcMetadata
from app.infrastructure.security.service_auth import ServiceAuthVerifier
from core.config import Settings


def _settings() -> Settings:
    return Settings(
        _env_file=None,
        env="test",
        casdoor_endpoint="https://identity.example.test",
        internal_auth_casdoor_application="sunmoonai-info-knowledge-ingest",
        internal_auth_audience="service-client",
        internal_auth_subject_allowlist="service-subject",
        internal_auth_required_scope="knowledge:ingest",
    )


def test_service_verifier_uses_explicit_service_discovery_url() -> None:
    settings = _settings().model_copy(
        update={
            "casdoor_discovery_url": "https://identity.example.test/.well-known/browser/openid-configuration",
            "internal_auth_discovery_url": "https://identity.example.test/.well-known/openid-configuration",
        }
    )
    verifier = ServiceAuthVerifier(settings)

    assert verifier._oidc._settings.casdoor_discovery_url == (
        "https://identity.example.test/.well-known/openid-configuration"
    )


def test_internal_discovery_uses_only_explicit_service_backchannel() -> None:
    settings = _settings().model_copy(
        update={
            "casdoor_endpoint": "https://browser-identity.example.test",
            "casdoor_backchannel_endpoint": "http://casdoor-sunmoonai:8000",
            "internal_auth_discovery_url": (
                "https://identity.example.test/.well-known/openid-configuration"
            ),
            "internal_auth_backchannel_endpoint": "http://casdoor-service:8000",
        }
    )
    verifier = ServiceAuthVerifier(settings)

    assert verifier._oidc._settings.casdoor_endpoint == "https://identity.example.test"
    assert verifier._oidc._settings.casdoor_backchannel_endpoint == (
        "http://casdoor-service:8000"
    )


def test_internal_discovery_does_not_inherit_browser_backchannel() -> None:
    settings = _settings().model_copy(
        update={
            "casdoor_backchannel_endpoint": "http://browser-casdoor:8000",
            "internal_auth_discovery_url": (
                "https://identity.example.test/.well-known/openid-configuration"
            ),
        }
    )
    verifier = ServiceAuthVerifier(settings)

    assert verifier._oidc._settings.casdoor_backchannel_endpoint is None


class FakeOidc:
    def __init__(self, key_set) -> None:
        self.key_set = key_set

    async def get_metadata(self):
        return OidcMetadata(
            issuer="https://identity.example.test",
            authorization_endpoint="https://identity.example.test/authorize",
            token_endpoint="https://identity.example.test/token",
            jwks_uri="https://identity.example.test/jwks",
        )

    async def get_key_set(self, metadata, *, force_refresh: bool = False):
        return self.key_set


def _token(key: RSAKey, **overrides) -> str:
    now = int(time.time())
    claims = {
        "iss": "https://identity.example.test",
        "sub": "service-subject",
        "aud": "service-client",
        "iat": now,
        "exp": now + 300,
        "scope": "knowledge:ingest",
    }
    claims.update(overrides)
    return jwt.encode(
        {"alg": "RS256", "kid": "test-key"},
        claims,
        key,
        algorithms=["RS256"],
    )


@pytest.mark.asyncio
async def test_service_token_is_verified_and_bound_to_relation() -> None:
    key = RSAKey.generate_key(parameters={"kid": "test-key"})
    verifier = ServiceAuthVerifier(_settings())
    verifier._oidc = FakeOidc(KeySet.import_key_set({"keys": [key.as_dict(private=False)]}))

    principal = await verifier.verify(_token(key))

    assert principal.actor_type == "service"
    assert principal.subject == "service-subject"
    assert principal.app == "knowledge"
    assert principal.surface == "internal"
    assert principal.scopes == frozenset({"knowledge:ingest"})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides, error",
    [
        ({"aud": "wrong-client"}, UnauthorizedError),
        ({"sub": "other-service"}, ForbiddenError),
        ({"scope": 42}, UnauthorizedError),
        ({"exp": int(time.time()) - 60}, UnauthorizedError),
    ],
)
async def test_service_token_negative_matrix(overrides: dict, error: type[Exception]) -> None:
    key = RSAKey.generate_key(parameters={"kid": "test-key"})
    verifier = ServiceAuthVerifier(_settings())
    verifier._oidc = FakeOidc(KeySet.import_key_set({"keys": [key.as_dict(private=False)]}))

    with pytest.raises(error):
        await verifier.verify(_token(key, **overrides))
