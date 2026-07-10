from __future__ import annotations

import uuid

from types import SimpleNamespace
from typing import Any, cast

from app.application.services.knowledge_ingestion_service import (
    PROCESSOR_NAME,
    build_idempotency_key,
    build_processing_success_metadata,
)
from app.interfaces.schemas.knowledge import KnowledgeIngestionCreate
from core.config import Settings


def test_build_idempotency_key_defaults_dataset() -> None:
    version_id = uuid.uuid4()
    payload = KnowledgeIngestionCreate(
        source_app="info-app",
        source_document_id=uuid.uuid4(),
        source_document_version_id=version_id,
    )

    assert build_idempotency_key(payload) == f"info-app:{version_id}:default"


def test_build_idempotency_key_uses_explicit_value() -> None:
    payload = KnowledgeIngestionCreate(
        source_app="info-app",
        source_document_id=uuid.uuid4(),
        source_document_version_id=uuid.uuid4(),
        target_dataset="market-news",
        idempotency_key="custom-key",
    )

    assert build_idempotency_key(payload) == "custom-key"


def test_ingestion_payload_uses_standard_contract_fields() -> None:
    document_id = uuid.uuid4()
    version_id = uuid.uuid4()

    payload = KnowledgeIngestionCreate.model_validate(
        {
            "source_document_id": str(document_id),
            "source_document_version_id": str(version_id),
            "canonical_url": "https://example.com/news",
            "title": "Example",
        }
    )

    assert payload.source_app == "info-app"
    assert payload.source_document_id == document_id
    assert payload.source_document_version_id == version_id
    assert payload.canonical_url == "https://example.com/news"


def test_database_url_uses_asyncpg_without_sslmode() -> None:
    settings = Settings(
        database_url=(
            "postgresql://knowledge:secret@postgresql:5432/knowledge"
            "?sslmode=require&connect_timeout=10"
        )
    )

    assert settings.database_url == (
        "postgresql+asyncpg://knowledge:secret@postgresql:5432/knowledge"
        "?connect_timeout=10"
    )


def test_build_processing_success_metadata_marks_mock_worker() -> None:
    job = SimpleNamespace(
        source_artifact_refs=[
            {"artifact_type": "clean"},
            {"artifact_type": "text"},
        ]
    )

    metadata = build_processing_success_metadata(cast(Any, job))

    assert metadata["processor"] == PROCESSOR_NAME
    assert metadata["mode"] == "mock"
    assert metadata["artifact_ref_count"] == 2
    assert metadata["ragflow"] == "deferred"
