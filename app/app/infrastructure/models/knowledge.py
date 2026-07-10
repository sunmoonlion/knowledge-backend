from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.models.base import Base, TimestampMixin, UUIDMixin


class KnowledgeIngestionJob(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "knowledge_ingestion_job"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_knowledge_ingestion_idempotency_key"),
        Index("ix_knowledge_ingestion_source_version", "source_app", "source_document_version_id"),
        Index("ix_knowledge_ingestion_status", "status"),
    )

    source_app: Mapped[str] = mapped_column(String(80), nullable=False)
    source_document_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_document_version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    target_dataset: Mapped[str | None] = mapped_column(String(255))
    profile_key: Mapped[str] = mapped_column(String(80), nullable=False, default="markdown")
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)

    title: Mapped[str | None] = mapped_column(Text)
    canonical_url: Mapped[str | None] = mapped_column(Text)
    source_name: Mapped[str | None] = mapped_column(String(255))
    content_hash: Mapped[str | None] = mapped_column(String(128))

    source_artifact_refs: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    status: Mapped[str] = mapped_column(String(30), nullable=False, default="accepted")
    last_error: Mapped[str | None] = mapped_column(Text)
    status_history: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    knowledge_document_id: Mapped[str | None] = mapped_column(String(255))
    ragflow_document_id: Mapped[str | None] = mapped_column(String(255))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
