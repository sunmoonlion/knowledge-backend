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


class FakeOidc:
    def __init__(self, key_set) -> None:
        self.key_set = key_set

    async def get_metadata(self):
        return OidcMetadata(
            issuer="https://identity.example.test/.well-known/sunmoonai-info-knowledge-ingest",
            authorization_endpoint="https://identity.example.test/authorize",
            token_endpoint="https://identity.example.test/token",
            jwks_uri="https://identity.example.test/jwks",
        )

    async def _get_key_set(self, metadata, *, force_refresh: bool = False):
        return self.key_set


def _token(key: RSAKey, **overrides) -> str:
    now = int(time.time())
    claims = {
        "iss": "https://identity.example.test/.well-known/sunmoonai-info-knowledge-ingest",
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
        ({"scope": "openid"}, ForbiddenError),
        ({"exp": int(time.time()) - 60}, UnauthorizedError),
    ],
)
async def test_service_token_negative_matrix(overrides: dict, error: type[Exception]) -> None:
    key = RSAKey.generate_key(parameters={"kid": "test-key"})
    verifier = ServiceAuthVerifier(_settings())
    verifier._oidc = FakeOidc(KeySet.import_key_set({"keys": [key.as_dict(private=False)]}))

    with pytest.raises(error):
        await verifier.verify(_token(key, **overrides))
