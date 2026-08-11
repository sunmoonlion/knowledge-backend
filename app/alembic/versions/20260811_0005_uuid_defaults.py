"""align UUID primary-key defaults with the ORM

Revision ID: 20260811_0005
Revises: 20260808_0004
Create Date: 2026-08-11
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "20260811_0005"
down_revision = "20260808_0004"
branch_labels = None
depends_on = None

TABLES = (
    "auth_user",
    "knowledge_ingestion_job",
    "knowledge_document",
    "knowledge_document_version",
)


def upgrade() -> None:
    for table in TABLES:
        op.alter_column(table, "id", server_default=sa.text("gen_random_uuid()"))


def downgrade() -> None:
    for table in reversed(TABLES):
        op.alter_column(table, "id", server_default=None)
