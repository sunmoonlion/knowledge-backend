"""knowledge ingestion jobs

Revision ID: 20260710_0001
Revises:
Create Date: 2026-07-10 08:45:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260710_0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "knowledge_ingestion_job",
        sa.Column("source_app", sa.String(length=80), nullable=False),
        sa.Column("source_document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_document_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_dataset", sa.String(length=255), nullable=True),
        sa.Column("profile_key", sa.String(length=80), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("canonical_url", sa.Text(), nullable=True),
        sa.Column("source_name", sa.String(length=255), nullable=True),
        sa.Column("content_hash", sa.String(length=128), nullable=True),
        sa.Column("source_artifact_refs", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("status_history", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("knowledge_document_id", sa.String(length=255), nullable=True),
        sa.Column("ragflow_document_id", sa.String(length=255), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_knowledge_ingestion_idempotency_key"),
    )
    op.create_index(
        "ix_knowledge_ingestion_source_version",
        "knowledge_ingestion_job",
        ["source_app", "source_document_version_id"],
        unique=False,
    )
    op.create_index(
        "ix_knowledge_ingestion_status",
        "knowledge_ingestion_job",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_knowledge_ingestion_status", table_name="knowledge_ingestion_job")
    op.drop_index("ix_knowledge_ingestion_source_version", table_name="knowledge_ingestion_job")
    op.drop_table("knowledge_ingestion_job")
