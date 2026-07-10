from __future__ import annotations

import uuid

from app.application.services.knowledge_ingestion_service import build_idempotency_key
from app.interfaces.schemas.knowledge import KnowledgeIngestionCreate


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


def test_ingestion_payload_accepts_info_app_distribution_fields() -> None:
    document_id = uuid.uuid4()
    version_id = uuid.uuid4()

    payload = KnowledgeIngestionCreate.model_validate(
        {
            "document_id": str(document_id),
            "version_id": str(version_id),
            "source_url": "https://example.com/news",
            "title": "Example",
        }
    )

    assert payload.source_app == "info-app"
    assert payload.source_document_id == document_id
    assert payload.source_document_version_id == version_id
    assert payload.canonical_url == "https://example.com/news"
