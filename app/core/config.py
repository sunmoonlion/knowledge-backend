from functools import lru_cache
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # 基础配置
    env: str = "development"
    log_level: str = "INFO"

    # 数据库（读 DATABASE_URL，自动补 +asyncpg 驱动前缀）
    database_url: str = "postgresql+asyncpg://knowledge:knowledge@localhost:5432/knowledge"
    # 仅 Alembic migration Job 使用；运行时 Deployment 不注入该值。
    migration_database_url: str | None = None

    @field_validator("database_url", "migration_database_url", mode="before")
    @classmethod
    def ensure_asyncpg(cls, v: str | None) -> str | None:
        if isinstance(v, str) and v.startswith("postgresql://"):
            v = v.replace("postgresql://", "postgresql+asyncpg://", 1)
        if isinstance(v, str) and v.startswith("postgresql+asyncpg://"):
            return strip_asyncpg_unsupported_query(v)
        return v

    # Redis（dbctl ACL 场景可设 REDIS_USER；仅 default 密码时可留空）
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_user: str | None = None
    redis_password: str | None = None

    # Casdoor BFF
    casdoor_endpoint: str = ""
    casdoor_client_id: str = ""
    casdoor_client_secret: str = ""
    casdoor_redirect_uri: str = ""
    casdoor_organization: str = "built-in"
    casdoor_application: str = "sunmoonai-knowledge-admin"
    casdoor_discovery_url: str | None = None
    casdoor_verify_ssl: bool = True

    auth_http_timeout_seconds: float = 10.0
    auth_transaction_ttl_seconds: int = 300
    auth_discovery_cache_seconds: int = 300
    auth_jwks_cache_seconds: int = 300
    auth_clock_skew_seconds: int = 30
    auth_allowed_algorithms: str = "RS256,ES256"
    auth_policy_version: str = "knowledge-admin-v1"
    session_cookie_secure: bool | None = None

    # Service-to-service resource server boundary.
    internal_auth_casdoor_application: str = "sunmoonai-info-knowledge-ingest"
    internal_auth_discovery_url: str | None = None
    internal_auth_audience: str | None = None
    internal_auth_subject_allowlist: str = ""
    internal_auth_required_scope: str = "knowledge:ingest"

    # Frontend
    # Used for post-login redirects from backend callback.
    frontend_base_url: str = "http://localhost:5173"
    frontend_allowed_origins: str | None = None

    # Session
    session_ttl_seconds: int = 3600

    @model_validator(mode="after")
    def validate_security_configuration(self) -> "Settings":
        raw_origins = self.frontend_allowed_origins or self.frontend_base_url
        if any(item.strip() == "*" for item in raw_origins.split(",")):
            raise ValueError("credential CORS cannot use wildcard origin")
        if self.env not in {"development", "test"}:
            if not self.casdoor_verify_ssl:
                raise ValueError("CASDOOR_VERIFY_SSL must be true in production")
            for field, value in (
                ("CASDOOR_ENDPOINT", self.casdoor_endpoint),
                ("CASDOOR_REDIRECT_URI", self.casdoor_redirect_uri),
                ("FRONTEND_BASE_URL", self.frontend_base_url),
            ):
                if value and urlsplit(value).scheme != "https":
                    raise ValueError(f"{field} must use HTTPS in production")
        return self

    @property
    def casdoor_discovery_endpoint(self) -> str:
        if self.casdoor_discovery_url:
            return self.casdoor_discovery_url
        if not self.casdoor_endpoint or not self.casdoor_application:
            return ""
        return (
            f"{self.casdoor_endpoint.rstrip('/')}/.well-known/"
            f"{self.casdoor_application}/openid-configuration"
        )

    @property
    def auth_allowed_algorithm_list(self) -> tuple[str, ...]:
        values = tuple(
            item.strip() for item in self.auth_allowed_algorithms.split(",") if item.strip()
        )
        if not values:
            raise ValueError("AUTH_ALLOWED_ALGORITHMS cannot be empty")
        allowed = {"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}
        if set(values) - allowed:
            raise ValueError(
                "AUTH_ALLOWED_ALGORITHMS must contain only configured asymmetric algorithms"
            )
        return values

    @property
    def frontend_origin_list(self) -> tuple[str, ...]:
        raw = self.frontend_allowed_origins or self.frontend_base_url
        values: list[str] = []
        for item in raw.split(","):
            parsed = urlsplit(item.strip())
            if not parsed.scheme or not parsed.hostname:
                continue
            port = f":{parsed.port}" if parsed.port is not None else ""
            values.append(f"{parsed.scheme}://{parsed.hostname}{port}")
        return tuple(dict.fromkeys(values))

    @property
    def auth_cookie_secure(self) -> bool:
        if self.session_cookie_secure is not None:
            return self.session_cookie_secure
        return self.env not in {"development", "test"}

    @property
    def internal_auth_subjects(self) -> frozenset[str]:
        return frozenset(
            item.strip()
            for item in self.internal_auth_subject_allowlist.split(",")
            if item.strip()
        )

    # Celery（应用层只读 CELERY_BROKER_URL；k8s 按 Deployment 注入 producer/worker 账号）
    celery_broker_url: str | None = Field(
        default=None, validation_alias="CELERY_BROKER_URL"
    )
    celery_queue: str = Field(
        default="default",
        validation_alias=AliasChoices("CELERY_QUEUE", "CELERY_TASK_DEFAULT_QUEUE"),
    )
    celery_result_backend: str | None = Field(
        default=None, validation_alias="CELERY_RESULT_BACKEND"
    )

    # RAGFlow ingestion（未配置 API key/base 时只验证 artifact，不伪造 ingestion 成功）
    ragflow_api_base: str | None = Field(default=None, validation_alias="RAGFLOW_API_BASE")
    ragflow_api_key: str | None = Field(default=None, validation_alias="RAGFLOW_API_KEY")
    ragflow_parse_timeout_seconds: int = Field(
        default=120, validation_alias="RAGFLOW_PARSE_TIMEOUT_SECONDS"
    )
    ragflow_parse_poll_interval_seconds: float = Field(
        default=1.0, validation_alias="RAGFLOW_PARSE_POLL_INTERVAL_SECONDS"
    )

    # S3 object storage（用于 ingestion worker 拉取上游 artifact）
    s3_endpoint: str | None = Field(default=None, validation_alias="S3_ENDPOINT")
    s3_region: str = Field(default="us-east-1", validation_alias="S3_REGION")
    s3_access_key_id: str | None = Field(default=None, validation_alias="S3_ACCESS_KEY_ID")
    s3_secret_access_key: str | None = Field(default=None, validation_alias="S3_SECRET_ACCESS_KEY")
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

    @property
    def celery_enabled(self) -> bool:
        return bool(self.celery_broker_url)

    @property
    def ragflow_enabled(self) -> bool:
        return bool(self.ragflow_api_base and self.ragflow_api_key)

    @property
    def artifact_bucket_allowlist(self) -> frozenset[str]:
        return frozenset(
            value.strip() for value in self.artifact_s3_allowed_buckets.split(",") if value.strip()
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


@lru_cache()
def get_settings() -> Settings:
    return Settings()


def strip_asyncpg_unsupported_query(database_url: str) -> str:
    parts = urlsplit(database_url)
    if not parts.query or "sslmode" not in parts.query:
        return database_url
    query = urlencode(
        [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key != "sslmode"],
        doseq=True,
    )
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))
