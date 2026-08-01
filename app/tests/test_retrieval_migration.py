from __future__ import annotations

import importlib.util
from pathlib import Path

from sqlalchemy.dialects.postgresql import asyncpg


def _load_migration_module():
    migration_path = (
        Path(__file__).parents[1]
        / "alembic"
        / "versions"
        / "20260715_0003_retrieval_domain.py"
    )
    spec = importlib.util.spec_from_file_location(
        "retrieval_domain_migration_20260715_0003", migration_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_legacy_binding_backfill_has_explicit_asyncpg_parameter_types() -> None:
    migration = _load_migration_module()

    compiled = str(
        migration._legacy_binding_backfill_statement().compile(
            dialect=asyncpg.dialect()
        )
    )

    assert "CAST($1 AS text)" in compiled
    assert "CAST($2 AS timestamptz)" in compiled
