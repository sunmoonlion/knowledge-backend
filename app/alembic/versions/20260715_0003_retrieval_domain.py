"""stable knowledge document and provider binding identity

Revision ID: 20260715_0003
Revises: 20260712_0002
Create Date: 2026-07-15
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260715_0003"
down_revision = "20260712_0002"
branch_labels = None
depends_on = None

_DOCUMENT_NAMESPACE = uuid.UUID("2d1540e8-d8b2-44b3-8da2-55f6c3027334")
_VERSION_NAMESPACE = uuid.UUID("9987a94c-9635-4124-b65e-738301c97831")


def _provider_dataset_id(row: dict) -> str | None:
    legacy = row.get("legacy_knowledge_document_id")
    if isinstance(legacy, str) and legacy.startswith("ragflow-dataset:"):
        return legacy.removeprefix("ragflow-dataset:")
    history = row.get("status_history") or []
    for entry in reversed(history if isinstance(history, list) else []):
        metadata = entry.get("metadata") if isinstance(entry, dict) else None
        value = metadata.get("ragflow_dataset_id") if isinstance(metadata, dict) else None
        if isinstance(value, str) and value:
            return value
    return None


def upgrade() -> None:
    op.create_table(
        "knowledge_document",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_app", sa.String(length=80), nullable=False),
        sa.Column("source_document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dataset_key", sa.String(length=120), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_app",
            "source_document_id",
            "dataset_key",
            name="uq_knowledge_document_source_dataset",
        ),
    )
    op.create_index(
        "ix_knowledge_document_dataset_status",
        "knowledge_document",
        ["dataset_key", "status"],
    )
    op.create_table(
        "knowledge_document_version",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("knowledge_document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_app", sa.String(length=80), nullable=False),
        sa.Column("source_document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_document_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("ingestion_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("dataset_key", sa.String(length=120), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("source_uri", sa.Text(), nullable=True),
        sa.Column("source_name", sa.String(length=255), nullable=True),
        sa.Column(
            "access_scope",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[\"tenant:sunmoonai\"]'::jsonb"),
        ),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("provider", sa.String(length=40), nullable=False),
        sa.Column("provider_dataset_id", sa.String(length=255), nullable=False),
        sa.Column("provider_document_id", sa.String(length=255), nullable=False),
        sa.Column("indexed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["knowledge_document_id"], ["knowledge_document.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["ingestion_id"], ["knowledge_ingestion_job.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "knowledge_document_id",
            "source_document_version_id",
            name="uq_knowledge_document_version_source",
        ),
        sa.UniqueConstraint("ingestion_id", name="uq_knowledge_document_version_ingestion"),
    )
    op.create_index(
        "ix_knowledge_version_dataset_status",
        "knowledge_document_version",
        ["dataset_key", "status"],
    )
    op.create_index(
        "ix_knowledge_version_provider_document",
        "knowledge_document_version",
        ["provider", "provider_document_id"],
    )
    op.create_index(
        "ix_knowledge_version_source_version",
        "knowledge_document_version",
        ["source_app", "source_document_version_id"],
    )

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            SELECT id, source_app, source_document_id, source_document_version_id,
                   COALESCE(target_dataset, 'default') AS dataset_key,
                   COALESCE(content_hash, repeat('0', 64)) AS content_hash,
                   title, canonical_url, source_name, ragflow_document_id,
                   knowledge_document_id AS legacy_knowledge_document_id,
                   status_history, COALESCE(completed_at, updated_at, created_at) AS indexed_at
              FROM knowledge_ingestion_job
             WHERE status = 'succeeded'
               AND ragflow_document_id IS NOT NULL
            """
        )
    ).mappings()
    now = datetime.now(UTC)
    for row_mapping in rows:
        row = dict(row_mapping)
        provider_dataset_id = _provider_dataset_id(row)
        if not provider_dataset_id:
            raise RuntimeError(
                f"cannot backfill provider dataset binding for ingestion {row['id']}"
            )
        document_id = uuid.uuid5(
            _DOCUMENT_NAMESPACE,
            f"{row['source_app']}:{row['source_document_id']}:{row['dataset_key']}",
        )
        version_id = uuid.uuid5(
            _VERSION_NAMESPACE,
            f"{document_id}:{row['source_document_version_id']}",
        )
        bind.execute(
            sa.text(
                """
                INSERT INTO knowledge_document
                    (id, source_app, source_document_id, dataset_key, status, created_at, updated_at)
                VALUES
                    (:id, :source_app, :source_document_id, :dataset_key, 'active', :now, :now)
                ON CONFLICT (source_app, source_document_id, dataset_key) DO NOTHING
                """
            ),
            {
                "id": document_id,
                "source_app": row["source_app"],
                "source_document_id": row["source_document_id"],
                "dataset_key": row["dataset_key"],
                "now": now,
            },
        )
        bind.execute(
            sa.text(
                """
                INSERT INTO knowledge_document_version
                    (id, knowledge_document_id, source_app, source_document_id,
                     source_document_version_id, ingestion_id, dataset_key, content_hash,
                     title, source_uri, source_name, access_scope, status, provider,
                     provider_dataset_id, provider_document_id, indexed_at, created_at, updated_at)
                VALUES
                    (:id, :knowledge_document_id, :source_app, :source_document_id,
                     :source_document_version_id, :ingestion_id, :dataset_key, :content_hash,
                     :title, :source_uri, :source_name, CAST(:access_scope AS jsonb), 'indexed',
                     'ragflow', :provider_dataset_id, :provider_document_id, :indexed_at,
                     :now, :now)
                ON CONFLICT (ingestion_id) DO NOTHING
                """
            ),
            {
                "id": version_id,
                "knowledge_document_id": document_id,
                "source_app": row["source_app"],
                "source_document_id": row["source_document_id"],
                "source_document_version_id": row["source_document_version_id"],
                "ingestion_id": row["id"],
                "dataset_key": row["dataset_key"],
                "content_hash": row["content_hash"],
                "title": row["title"],
                "source_uri": row["canonical_url"],
                "source_name": row["source_name"],
                "access_scope": '["tenant:sunmoonai"]',
                "provider_dataset_id": provider_dataset_id,
                "provider_document_id": row["ragflow_document_id"],
                "indexed_at": row["indexed_at"] or now,
                "now": now,
            },
        )
        bind.execute(
            sa.text(
                "UPDATE knowledge_ingestion_job SET knowledge_document_id = :document_id WHERE id = :id"
            ),
            {"document_id": str(document_id), "id": row["id"]},
        )

    # Historical Phase-0 mock/verification rows used `succeeded` without any
    # real provider binding. They must not retain production-success semantics
    # after the retrieval boundary becomes available.
    bind.execute(
        sa.text(
            """
            UPDATE knowledge_ingestion_job AS job
               SET status = 'legacy_binding_missing',
                   last_error = 'legacy succeeded record has no provider binding; re-ingestion required',
                   status_history = COALESCE(status_history, '[]'::jsonb) ||
                       jsonb_build_array(jsonb_build_object(
                           'status', 'legacy_binding_missing',
                           'at', :at,
                           'last_error', 'legacy succeeded record has no provider binding; re-ingestion required',
                           'metadata', jsonb_build_object(
                               'migration', '20260715_0003',
                               'error_type', 'provider_binding_missing'
                           )
                       )),
                   updated_at = :now
             WHERE job.status = 'succeeded'
               AND NOT EXISTS (
                   SELECT 1
                     FROM knowledge_document_version AS version
                    WHERE version.ingestion_id = job.id
               )
            """
        ),
        {"at": now.isoformat(), "now": now},
    )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            UPDATE knowledge_ingestion_job
               SET status = 'succeeded',
                   last_error = NULL,
                   updated_at = now()
             WHERE status = 'legacy_binding_missing'
               AND last_error = 'legacy succeeded record has no provider binding; re-ingestion required'
            """
        )
    )
    bind.execute(
        sa.text(
            """
            UPDATE knowledge_ingestion_job AS job
               SET knowledge_document_id = 'ragflow-dataset:' || version.provider_dataset_id
              FROM knowledge_document_version AS version
             WHERE version.ingestion_id = job.id
            """
        )
    )
    op.drop_index("ix_knowledge_version_source_version", table_name="knowledge_document_version")
    op.drop_index("ix_knowledge_version_provider_document", table_name="knowledge_document_version")
    op.drop_index("ix_knowledge_version_dataset_status", table_name="knowledge_document_version")
    op.drop_table("knowledge_document_version")
    op.drop_index("ix_knowledge_document_dataset_status", table_name="knowledge_document")
    op.drop_table("knowledge_document")
