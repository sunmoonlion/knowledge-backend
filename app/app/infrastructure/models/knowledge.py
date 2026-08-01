from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, String, Text, UniqueConstraint
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


class KnowledgeDocument(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "knowledge_document"
    __table_args__ = (
        UniqueConstraint(
            "source_app",
            "source_document_id",
            "dataset_key",
            name="uq_knowledge_document_source_dataset",
        ),
        Index("ix_knowledge_document_dataset_status", "dataset_key", "status"),
    )

    source_app: Mapped[str] = mapped_column(String(80), nullable=False)
    source_document_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    dataset_key: Mapped[str] = mapped_column(String(120), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="active")


class KnowledgeDocumentVersion(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "knowledge_document_version"
    __table_args__ = (
        UniqueConstraint(
            "knowledge_document_id",
            "source_document_version_id",
            name="uq_knowledge_document_version_source",
        ),
        UniqueConstraint("ingestion_id", name="uq_knowledge_document_version_ingestion"),
        Index("ix_knowledge_version_dataset_status", "dataset_key", "status"),
        Index("ix_knowledge_version_provider_document", "provider", "provider_document_id"),
        Index("ix_knowledge_version_source_version", "source_app", "source_document_version_id"),
    )

    knowledge_document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("knowledge_document.id", ondelete="RESTRICT"),
        nullable=False,
    )
    source_app: Mapped[str] = mapped_column(String(80), nullable=False)
    source_document_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_document_version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("knowledge_ingestion_job.id", ondelete="RESTRICT"),
        nullable=False,
    )
    dataset_key: Mapped[str] = mapped_column(String(120), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str | None] = mapped_column(Text)
    source_uri: Mapped[str | None] = mapped_column(Text)
    source_name: Mapped[str | None] = mapped_column(String(255))
    access_scope: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="indexed")
    provider: Mapped[str] = mapped_column(String(40), nullable=False)
    provider_dataset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    provider_document_id: Mapped[str] = mapped_column(String(255), nullable=False)
    indexed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
