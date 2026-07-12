from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import Header
from joserfc import jwt
from joserfc.errors import JoseError
from joserfc.jwt import JWTClaimsRegistry

from app.application.errors.exceptions import ForbiddenError, ServiceUnavailableError, UnauthorizedError
from app.domain.security import Principal
from app.infrastructure.security import OidcProviderClient
from core.config import Settings, get_settings


class ServiceAuthVerifier:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        service_settings = self._settings.model_copy(
            update={
                "casdoor_application": self._settings.internal_auth_casdoor_application,
                "casdoor_client_id": self._settings.internal_auth_audience or "",
                "casdoor_client_secret": "",
                "casdoor_redirect_uri": "",
            }
        )
        self._oidc = OidcProviderClient(service_settings)

    async def verify(self, encoded: str) -> Principal:
        expected_audience = self._settings.internal_auth_audience
        allowed_subjects = self._settings.internal_auth_subjects
        if not expected_audience or not allowed_subjects:
            raise ServiceUnavailableError("internal service identity binding is not configured")

        metadata = await self._oidc.get_metadata()
        last_error: Exception | None = None
        for refresh in (False, True):
            try:
                key_set = await self._oidc.get_key_set(metadata, force_refresh=refresh)
                token = jwt.decode(
                    encoded,
                    key_set,
                    algorithms=self._settings.auth_allowed_algorithm_list,
                )
                claims = token.claims
                JWTClaimsRegistry(
                    leeway=self._settings.auth_clock_skew_seconds,
                    iss={"essential": True, "value": metadata.issuer},
                    sub={"essential": True},
                    aud={"essential": True},
                    exp={"essential": True},
                    iat={"essential": True},
                ).validate(claims)
                audience = claims.get("aud")
                if audience != expected_audience and audience != [expected_audience]:
                    raise UnauthorizedError("service token audience mismatch")
                subject = claims.get("sub")
                if not isinstance(subject, str) or subject not in allowed_subjects:
                    raise ForbiddenError("service subject is not bound")
                self._validate_scope(claims)
                expires_at_epoch = int(claims["exp"])
                return Principal(
                    actor_type="service",
                    subject=subject,
                    issuer=metadata.issuer,
                    app="knowledge",
                    surface="internal",
                    audience=expected_audience,
                    roles=(),
                    scopes=frozenset({self._settings.internal_auth_required_scope}),
                    authenticated_at=datetime.fromtimestamp(int(claims["iat"]), tz=UTC),
                    expires_at=datetime.fromtimestamp(expires_at_epoch, tz=UTC),
                    policy_version=self._settings.auth_policy_version,
                )
            except (UnauthorizedError, ForbiddenError):
                raise
            except (JoseError, ValueError, TypeError) as exc:
                last_error = exc
        raise UnauthorizedError("service token invalid") from last_error

    def _validate_scope(self, claims: dict[str, Any]) -> None:
        # Casdoor client-credentials tokens may expose only the provider's
        # `openid` scope. The relation-specific scope is granted by the local
        # subject allowlist and represented on the returned Principal.
        raw_scope = claims.get("scope", claims.get("scp"))
        if raw_scope is None:
            return
        if isinstance(raw_scope, str):
            return
        elif isinstance(raw_scope, list):
            if not all(isinstance(item, str) for item in raw_scope):
                raise UnauthorizedError("service token scope invalid")
            return
        else:
            raise UnauthorizedError("service token scope invalid")


_verifier: ServiceAuthVerifier | None = None


def get_service_auth_verifier() -> ServiceAuthVerifier:
    global _verifier
    if _verifier is None:
        _verifier = ServiceAuthVerifier()
    return _verifier


async def require_knowledge_ingest_service(
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> Principal:
    if not authorization:
        raise UnauthorizedError("service bearer token required")
    try:
        scheme, encoded = authorization.split(" ", 1)
    except ValueError as exc:
        raise UnauthorizedError("service bearer token required") from exc
    if scheme.lower() != "bearer" or not encoded.strip():
        raise UnauthorizedError("service bearer token required")
    return await get_service_auth_verifier().verify(encoded.strip())
