import importlib.util
from pathlib import Path


def test_delivery_migration_declares_uuid_dependency_before_backfill(monkeypatch):
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic/versions/20260911_0006_durable_delivery.py"
    )
    spec = importlib.util.spec_from_file_location(
        "knowledge_uuid_dependency", migration_path
    )
    assert spec and spec.loader
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    monkeypatch.setattr(migration, "shared_upgrade", lambda: None)
    migration.upgrade()
    assert (
        statements[0] == 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public'
    )
    assert any("uuid_generate_v5" in statement for statement in statements[1:])
