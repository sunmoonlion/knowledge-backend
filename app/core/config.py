from __future__ import annotations

from functools import lru_cache
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration shared by the API and optional Celery worker.

    Browser identity settings are validated explicitly by
    :meth:`require_browser_identity` so a worker can start without receiving a
    browser client secret. The API calls that method before opening external
    connections.
    """

    env: str = "development"
    log_level: str = "INFO"
    service_name: str = "knowledge-admin-backend"
    app_slug: str = "knowledge"
    surface: str = "admin"

    database_url: str = "postgresql+asyncpg://knowledge:knowledge@localhost:5432/knowledge"
    migration_database_url: str | None = None

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_user: str | None = None
    redis_password: str | None = None

    casdoor_endpoint: str = ""
    casdoor_discovery_url: str | None = None
    casdoor_backchannel_endpoint: str | None = None
    casdoor_client_id: str = ""
    casdoor_client_secret: str = ""
    casdoor_redirect_uri: str = ""
    casdoor_organization: str = "built-in"
    casdoor_application: str = "sunmoonai-knowledge-admin"
    casdoor_verify_ssl: bool = True

    auth_http_timeout_seconds: float = Field(default=10.0, gt=0, le=60)
    auth_transaction_ttl_seconds: int = Field(default=300, ge=60, le=900)
    auth_discovery_cache_seconds: int = Field(default=300, ge=1, le=3600)
    auth_jwks_cache_seconds: int = Field(default=300, ge=1, le=3600)
    auth_clock_skew_seconds: int = Field(default=30, ge=0, le=120)
    auth_allowed_algorithms: str = "RS256,ES256"
    auth_policy_version: str = "knowledge-admin-v1"
    auth_role_allowlist: str = ""
    auth_scope_allowlist: str = ""

    frontend_base_url: str = "http://localhost:5173"
    frontend_allowed_origins: str | None = None
    allowed_hosts: str = "localhost,127.0.0.1,testserver"

    session_ttl_seconds: int = Field(default=3600, ge=60, le=86400)
    session_cookie_secure: bool | None = None

    celery_broker_url: str | None = Field(
        default=None, validation_alias="CELERY_BROKER_URL"
    )
    celery_queue: str = Field(
        default="knowledge.admin.default",
        validation_alias=AliasChoices("CELERY_QUEUE", "CELERY_TASK_DEFAULT_QUEUE"),
    )
    celery_result_backend: str | None = Field(
        default=None, validation_alias="CELERY_RESULT_BACKEND"
    )

    # Service-to-service resource server boundary (Info -> Knowledge ingest).
    # Cross-app relation names must NOT be rewritten to knowledge-*.
    internal_auth_casdoor_application: str = "sunmoonai-info-knowledge-ingest"
    internal_auth_discovery_url: str | None = None
    internal_auth_backchannel_endpoint: str | None = None
    internal_auth_audience: str | None = None
    internal_auth_subject_allowlist: str = ""
    internal_auth_required_scope: str = "knowledge:ingest"

    # Independent Research worker -> Knowledge retrieval resource boundary.
    retrieval_auth_casdoor_application: str = "sunmoonai-research-knowledge-retrieve"
    retrieval_auth_discovery_url: str | None = None
    retrieval_auth_backchannel_endpoint: str | None = None
    retrieval_auth_audience: str | None = None
    retrieval_auth_subject_allowlist: str = ""
    retrieval_auth_required_scope: str = "knowledge:retrieve"

    # RAGFlow ingestion (unconfigured base/key only validates artifact; never fake success).
    ragflow_api_base: str | None = Field(default=None, validation_alias="RAGFLOW_API_BASE")
    ragflow_api_key: str | None = Field(default=None, validation_alias="RAGFLOW_API_KEY")
    ragflow_parse_timeout_seconds: int = Field(
        default=120, validation_alias="RAGFLOW_PARSE_TIMEOUT_SECONDS"
    )
    ragflow_parse_poll_interval_seconds: float = Field(
        default=1.0, validation_alias="RAGFLOW_PARSE_POLL_INTERVAL_SECONDS"
    )
    retrieval_dataset_allowlist: str = Field(
        default="default",
        validation_alias="RETRIEVAL_DATASET_ALLOWLIST",
    )
    retrieval_default_tenant_id: str = Field(
        default="sunmoonai",
        validation_alias="RETRIEVAL_DEFAULT_TENANT_ID",
    )
    retrieval_provider_timeout_seconds: float = Field(
        default=15.0,
        gt=0,
        le=120,
        validation_alias="RETRIEVAL_PROVIDER_TIMEOUT_SECONDS",
    )

    # S3 object storage (ingestion worker pulls upstream artifacts).
    s3_endpoint: str | None = Field(default=None, validation_alias="S3_ENDPOINT")
    s3_region: str = Field(default="us-east-1", validation_alias="S3_REGION")
    s3_access_key_id: str | None = Field(default=None, validation_alias="S3_ACCESS_KEY_ID")
    s3_secret_access_key: str | None = Field(
        default=None, validation_alias="S3_SECRET_ACCESS_KEY"
    )
    s3_force_path_style: bool = Field(default=True, validation_alias="S3_FORCE_PATH_STYLE")
    artifact_s3_allowed_buckets: str = Field(
        default="development-info-originals",
        validation_alias="ARTIFACT_S3_ALLOWED_BUCKETS",
    )
    artifact_s3_allowed_prefixes: str = Field(
        default="info/original/",
        validation_alias="ARTIFACT_S3_ALLOWED_PREFIXES",
    )
    artifact_max_size_bytes: int = Field(
        default=52_428_800,
        ge=1,
        le=52_428_800,
        validation_alias="ARTIFACT_MAX_SIZE_BYTES",
    )
    artifact_allowed_content_types: str = Field(
        default="text/markdown,text/plain",
        validation_alias="ARTIFACT_ALLOWED_CONTENT_TYPES",
    )

    @field_validator("database_url", "migration_database_url", mode="before")
    @classmethod
    def normalize_postgres_url(cls, value: str | None) -> str | None:
        if not isinstance(value, str) or not (
            value.startswith("postgresql://")
            or value.startswith("postgresql+asyncpg://")
        ):
            return value
        normalized = value.replace("postgresql://", "postgresql+asyncpg://", 1)
        parts = urlsplit(normalized)
        query = [
            (key, item)
            for key, item in parse_qsl(parts.query, keep_blank_values=True)
            if key != "sslmode"
        ]
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                urlencode(query),
                parts.fragment,
            )
        )

    @model_validator(mode="after")
    def validate_base_security(self) -> Settings:
        if self.surface != "admin":
            raise ValueError("tpl-admin-backend requires SURFACE=admin")
        if not self.app_slug or any(
            char not in "abcdefghijklmnopqrstuvwxyz0123456789-"
            for char in self.app_slug
        ):
            raise ValueError("APP_SLUG must use lowercase letters, digits and hyphens")
        origins = tuple(
            self._strict_origin(item, field="FRONTEND_ALLOWED_ORIGINS")
            for item in self._split_csv(self.frontend_origins_raw)
        )
        if not origins:
            raise ValueError(
                "FRONTEND_ALLOWED_ORIGINS must contain an absolute HTTP(S) origin"
            )
        if self.is_production and any(
            urlsplit(origin).scheme != "https" for origin in origins
        ):
            raise ValueError("FRONTEND_ALLOWED_ORIGINS must use HTTPS in production")
        if any(item.strip() == "*" for item in self._split_csv(self.allowed_hosts)):
            if self.is_production:
                raise ValueError("ALLOWED_HOSTS cannot use wildcard in production")
        if self.is_production and not self.casdoor_verify_ssl:
            raise ValueError("CASDOOR_VERIFY_SSL must be true in production")
        if self.is_production and self.session_cookie_secure is False:
            raise ValueError("SESSION_COOKIE_SECURE cannot be false in production")
        return self

    @staticmethod
    def _split_csv(value: str) -> tuple[str, ...]:
        return tuple(item.strip() for item in value.split(",") if item.strip())

    @staticmethod
    def _strict_origin(value: str, *, field: str) -> str:
        if value == "*":
            raise ValueError(f"{field} cannot contain wildcard origin")
        try:
            parsed = urlsplit(value)
            port = f":{parsed.port}" if parsed.port is not None else ""
        except ValueError as exc:
            raise ValueError(f"{field} contains an invalid port") from exc
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(f"{field} must contain origin-only HTTP(S) URLs")
        return f"{parsed.scheme}://{parsed.hostname}{port}"

    @property
    def is_production(self) -> bool:
        return self.env not in {"development", "test"}

    @property
    def migration_url(self) -> str:
        return self.migration_database_url or self.database_url

    @property
    def casdoor_discovery_endpoint(self) -> str:
        if self.casdoor_discovery_url:
            return self.casdoor_discovery_url
        if not self.casdoor_endpoint:
            return ""
        return f"{self.casdoor_endpoint.rstrip('/')}/.well-known/openid-configuration"

    @property
    def auth_allowed_algorithm_list(self) -> tuple[str, ...]:
        values = self._split_csv(self.auth_allowed_algorithms)
        if not values:
            raise ValueError("AUTH_ALLOWED_ALGORITHMS cannot be empty")
        allowed = {"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}
        if set(values) - allowed:
            raise ValueError(
                "AUTH_ALLOWED_ALGORITHMS must contain only asymmetric algorithms"
            )
        return values

    @property
    def frontend_origins_raw(self) -> str:
        return self.frontend_allowed_origins or self.frontend_base_url

    @property
    def frontend_origin_list(self) -> tuple[str, ...]:
        origins = [
            self._strict_origin(item, field="FRONTEND_ALLOWED_ORIGINS")
            for item in self._split_csv(self.frontend_origins_raw)
        ]
        return tuple(dict.fromkeys(origins))

    @property
    def auth_role_allowlist_items(self) -> frozenset[str]:
        return frozenset(self._split_csv(self.auth_role_allowlist))

    @property
    def auth_scope_allowlist_items(self) -> frozenset[str]:
        return frozenset(self._split_csv(self.auth_scope_allowlist))

    @property
    def allowed_host_list(self) -> tuple[str, ...]:
        return self._split_csv(self.allowed_hosts)

    @property
    def auth_cookie_secure(self) -> bool:
        if self.session_cookie_secure is not None:
            return self.session_cookie_secure
        return self.is_production

    @property
    def session_cookie_name(self) -> str:
        return f"sunmoonai_{self.app_slug}_{self.surface}_sid"

    @property
    def transaction_cookie_name(self) -> str:
        return f"sunmoonai_{self.app_slug}_{self.surface}_oidc_tx"

    @property
    def session_key_prefix(self) -> str:
        return f"{self.app_slug}:auth:{self.surface}:session:"

    @property
    def transaction_key_prefix(self) -> str:
        return f"{self.app_slug}:auth:{self.surface}:oidc:"

    @property
    def required_admin_scope(self) -> str:
        return f"{self.app_slug}:admin"

    @property
    def celery_enabled(self) -> bool:
        return bool(self.celery_broker_url)

    def require_browser_identity(self) -> None:
        required = {
            "CASDOOR_ENDPOINT": self.casdoor_endpoint,
            "CASDOOR_CLIENT_ID": self.casdoor_client_id,
            "CASDOOR_CLIENT_SECRET": self.casdoor_client_secret,
            "CASDOOR_REDIRECT_URI": self.casdoor_redirect_uri,
            "FRONTEND_BASE_URL": self.frontend_base_url,
        }
        missing = sorted(name for name, value in required.items() if not value.strip())
        if missing:
            raise ValueError(
                f"browser identity configuration missing: {', '.join(missing)}"
            )
        provider_origin = self._strict_origin(
            self.casdoor_endpoint, field="CASDOOR_ENDPOINT"
        )
        discovery = urlsplit(self.casdoor_discovery_endpoint)
        if (
            discovery.scheme not in {"http", "https"}
            or not discovery.hostname
            or discovery.username
            or discovery.password
            or discovery.query
            or discovery.fragment
            or discovery.path != "/.well-known/openid-configuration"
        ):
            raise ValueError(
                "CASDOOR_DISCOVERY_URL must use the standard discovery path"
            )
        discovery_origin = f"{discovery.scheme}://{discovery.hostname}" + (
            f":{discovery.port}" if discovery.port is not None else ""
        )
        if discovery_origin != provider_origin:
            raise ValueError(
                "CASDOOR_DISCOVERY_URL must use the CASDOOR_ENDPOINT origin"
            )
        if self.is_production and urlsplit(provider_origin).scheme != "https":
            raise ValueError("CASDOOR_ENDPOINT must use HTTPS in production")

        redirect = urlsplit(self.casdoor_redirect_uri)
        if (
            redirect.scheme not in {"http", "https"}
            or not redirect.hostname
            or redirect.username
            or redirect.password
            or redirect.query
            or redirect.fragment
            or redirect.path != "/api/auth/callback"
        ):
            raise ValueError("CASDOOR_REDIRECT_URI must end at /api/auth/callback")
        if self.is_production and redirect.scheme != "https":
            raise ValueError("CASDOOR_REDIRECT_URI must use HTTPS in production")
        frontend_origin = self._strict_origin(
            self.frontend_base_url, field="FRONTEND_BASE_URL"
        )
        if self.is_production and urlsplit(frontend_origin).scheme != "https":
            raise ValueError("FRONTEND_BASE_URL must use HTTPS in production")
        if (
            f"{redirect.scheme}://{redirect.hostname}"
            + (f":{redirect.port}" if redirect.port is not None else "")
            != frontend_origin
        ):
            raise ValueError(
                "CASDOOR_REDIRECT_URI must use the FRONTEND_BASE_URL origin"
            )
        if not self.frontend_origin_list:
            raise ValueError("FRONTEND_ALLOWED_ORIGINS must contain an absolute origin")
        if frontend_origin not in self.frontend_origin_list:
            raise ValueError("FRONTEND_ALLOWED_ORIGINS must include FRONTEND_BASE_URL")
        if not self.allowed_host_list:
            raise ValueError("ALLOWED_HOSTS cannot be empty")
        # Force algorithm validation during startup rather than first callback.
        _ = self.auth_allowed_algorithm_list


    @property
    def internal_auth_subjects(self) -> frozenset[str]:
        return frozenset(
            item.strip()
            for item in self.internal_auth_subject_allowlist.split(",")
            if item.strip()
        )

    @property
    def retrieval_auth_subjects(self) -> frozenset[str]:
        return frozenset(
            item.strip()
            for item in self.retrieval_auth_subject_allowlist.split(",")
            if item.strip()
        )

    @property
    def ragflow_enabled(self) -> bool:
        return bool(self.ragflow_api_base and self.ragflow_api_key)

    @property
    def retrieval_datasets(self) -> frozenset[str]:
        return frozenset(
            value.strip()
            for value in self.retrieval_dataset_allowlist.split(",")
            if value.strip()
        )

    @property
    def artifact_bucket_allowlist(self) -> frozenset[str]:
        return frozenset(
            value.strip()
            for value in self.artifact_s3_allowed_buckets.split(",")
            if value.strip()
        )

    @property
    def artifact_prefix_allowlist(self) -> tuple[str, ...]:
        return tuple(
            value.strip().lstrip("/")
            for value in self.artifact_s3_allowed_prefixes.split(",")
            if value.strip()
        )

    @property
    def artifact_content_type_allowlist(self) -> frozenset[str]:
        return frozenset(
            value.strip().lower()
            for value in self.artifact_allowed_content_types.split(",")
            if value.strip()
        )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
