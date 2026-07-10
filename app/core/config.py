from functools import lru_cache
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # 基础配置
    env: str = "development"
    log_level: str = "INFO"

    # 数据库（读 DATABASE_URL，自动补 +asyncpg 驱动前缀）
    database_url: str = "postgresql+asyncpg://knowledge:knowledge@localhost:5432/knowledge"

    @field_validator("database_url", mode="before")
    @classmethod
    def ensure_asyncpg(cls, v: str) -> str:
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
    casdoor_application: str = "app-knowledge"
    casdoor_verify_ssl: bool = True

    # Frontend
    # Used for post-login redirects from backend callback.
    frontend_base_url: str = "http://localhost:5173"

    # Session
    session_ttl_seconds: int = 3600

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

    # RAGFlow ingestion（未配置 API key/base 时 worker 保持 mock 模式）
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

    @property
    def celery_enabled(self) -> bool:
        return bool(self.celery_broker_url)

    @property
    def ragflow_enabled(self) -> bool:
        return bool(self.ragflow_api_base and self.ragflow_api_key)

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
