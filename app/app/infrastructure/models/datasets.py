from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.models.base import Base, TimestampMixin, UUIDMixin


class KnowledgeDataset(UUIDMixin, TimestampMixin, Base):
    """登记的数据集版本（0008-info 段三）。每个数据集同一时刻只有一个现行版本。"""

    __tablename__ = "knowledge_dataset"

    dataset_id: Mapped[str] = mapped_column(String(80), nullable=False)
    data_version: Mapped[str] = mapped_column(String(160), nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    security_code: Mapped[str | None] = mapped_column(String(6))
    bucket: Mapped[str] = mapped_column(String(63), nullable=False)
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    object_version_id: Mapped[str | None] = mapped_column(String(255))
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    start_date: Mapped[str] = mapped_column(String(10), nullable=False)
    end_date: Mapped[str] = mapped_column(String(10), nullable=False)
    source_app: Mapped[str] = mapped_column(String(80), nullable=False)
    source_ref: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    registered_by: Mapped[str] = mapped_column(String(255), nullable=False)
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "dataset_id", "data_version", name="uq_knowledge_dataset_version"
        ),
        CheckConstraint(
            "status IN ('active', 'superseded')", name="ck_knowledge_dataset_status"
        ),
        CheckConstraint(
            "sha256 ~ '^[0-9a-f]{64}$'", name="ck_knowledge_dataset_sha256"
        ),
        Index(
            "uq_knowledge_dataset_one_active",
            "dataset_id",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
    )
