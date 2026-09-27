"""Dataset registry: one active version per dataset, older versions superseded."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260927_0007"
down_revision = "20260911_0006"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "knowledge_dataset",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("dataset_id", sa.String(80), nullable=False),
        sa.Column("data_version", sa.String(160), nullable=False),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("security_code", sa.String(6)),
        sa.Column("bucket", sa.String(63), nullable=False),
        sa.Column("object_key", sa.Text(), nullable=False),
        sa.Column("object_version_id", sa.String(255)),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("start_date", sa.String(10), nullable=False),
        sa.Column("end_date", sa.String(10), nullable=False),
        sa.Column("source_app", sa.String(80), nullable=False),
        sa.Column("source_ref", sa.String(255)),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("registered_by", sa.String(255), nullable=False),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "dataset_id", "data_version", name="uq_knowledge_dataset_version"
        ),
        sa.CheckConstraint(
            "status IN ('active', 'superseded')", name="ck_knowledge_dataset_status"
        ),
        sa.CheckConstraint(
            "sha256 ~ '^[0-9a-f]{64}$'", name="ck_knowledge_dataset_sha256"
        ),
    )
    op.create_index(
        "uq_knowledge_dataset_one_active",
        "knowledge_dataset",
        ["dataset_id"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )


def downgrade():
    op.drop_index("uq_knowledge_dataset_one_active", table_name="knowledge_dataset")
    op.drop_table("knowledge_dataset")
