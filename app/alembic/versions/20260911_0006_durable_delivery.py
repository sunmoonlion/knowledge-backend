"""Adopt shared delivery and preserve unknown historical provider outcomes."""

import sqlalchemy as sa

from alembic import op
from app.infrastructure.messaging.delivery_schema import downgrade as shared_downgrade
from app.infrastructure.messaging.delivery_schema import upgrade as shared_upgrade

revision = "20260911_0006"
down_revision = "20260811_0005"
branch_labels = None
depends_on = None


def upgrade():
    shared_upgrade()
    op.execute("""
        CREATE TABLE knowledge_provider_operation (
            operation_key varchar(512) PRIMARY KEY,
            intent jsonb NOT NULL,
            state varchar(30) NOT NULL CHECK
                (state IN ('intent','executing','unknown','confirmed',
                           'legacy_unknown')),
            receipt jsonb NOT NULL DEFAULT '{}',
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )
    """)
    # An old running worker may have uploaded without saving any receipt. Never
    # reinterpret that absence as permission to upload the source version again.
    op.execute("""
        INSERT INTO knowledge_provider_operation(operation_key,intent,state)
        SELECT 'upload:' || uuid_generate_v5(
            '6ba7b811-9dad-11d1-80b4-00c04fd430c8'::uuid,
            'knowledge-upload:' || source_app || ':' || source_document_version_id
                || ':' || COALESCE(target_dataset,'default'))::text,
            jsonb_build_object('migration','20260911_0006'), 'legacy_unknown'
        FROM knowledge_ingestion_job WHERE status='running'
        ON CONFLICT DO NOTHING
    """)
    op.execute("""
        INSERT INTO outbox_message
            (id,topic,aggregate_key,payload,headers,deduplication_key,available_at)
        SELECT uuid_generate_v4(), 'knowledge.ingest.v1',
            uuid_generate_v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8'::uuid,
                'knowledge-upload:' || source_app || ':' || source_document_version_id
                    || ':' || COALESCE(target_dataset,'default'))::text,
            jsonb_build_object('ingestion_id', id::text), '{}',
            'knowledge:ingest:' || id || ':' ||
                COALESCE(metadata_json->>'retry_count','0'),
            now()
        FROM knowledge_ingestion_job WHERE status IN ('accepted','running')
        ON CONFLICT (deduplication_key) DO NOTHING
    """)


def downgrade():
    connection = op.get_bind()
    pending = connection.execute(
        sa.text("""
        SELECT count(*) FROM outbox_message m
        WHERE m.topic='knowledge.ingest.v1' AND NOT EXISTS
            (SELECT 1 FROM inbox_message i
                WHERE i.message_id=m.id AND i.consumer=m.topic)
    """)
    ).scalar_one()
    if pending:
        raise RuntimeError("drain Knowledge commands before downgrade")
    if connection.execute(
        sa.text("SELECT count(*) FROM knowledge_provider_operation")
    ).scalar_one():
        raise RuntimeError(
            "provider receipts require a verified backup restore; "
            "downgrade refuses receipt loss"
        )
    op.execute("DROP TABLE knowledge_provider_operation")
    shared_downgrade()
