from __future__ import annotations

import uuid

import httpx
from types import SimpleNamespace
from typing import Any, cast

from app.application.services.knowledge_ingestion_service import (
    PROCESSOR_NAME,
    RAGFLOW_PROCESSOR_NAME,
    build_idempotency_key,
    build_processing_success_metadata,
    build_ragflow_success_metadata,
)
from app.infrastructure.external.ragflow import (
    ArtifactContent,
    RAGFlowClient,
    _s3_sigv4_headers,
    ingest_into_ragflow,
    resolve_artifact_content,
)
from app.infrastructure.messaging.celery_producer import CeleryProducer
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


def test_build_ragflow_success_metadata_marks_processed() -> None:
    job = SimpleNamespace(source_artifact_refs=[{"artifact_type": "text"}])

    metadata = build_ragflow_success_metadata(
        cast(Any, job),
        {
            "ragflow_dataset_id": "dataset-1",
            "ragflow_document_name": "doc.txt",
            "ragflow_chunk_count": 3,
        },
    )

    assert metadata["processor"] == RAGFLOW_PROCESSOR_NAME
    assert metadata["mode"] == "ragflow"
    assert metadata["ragflow"] == "processed"
    assert metadata["ragflow_dataset_id"] == "dataset-1"
    assert metadata["ragflow_chunk_count"] == 3


async def test_resolve_artifact_content_uses_inline_metadata() -> None:
    artifact = await resolve_artifact_content(
        settings=Settings(),
        source_artifact_refs=[],
        title="Inline Smoke",
        canonical_url=None,
        metadata_json={"markdown": "# hello"},
        source_document_version_id="version-1",
    )

    assert isinstance(artifact, ArtifactContent)
    assert artifact.filename == "Inline-Smoke.txt"
    assert artifact.content == b"# hello"
    assert artifact.content_type == "text/plain; charset=utf-8"


def test_s3_sigv4_headers_include_signed_authorization() -> None:
    headers = _s3_sigv4_headers(
        method="GET",
        host="minio.example.test",
        canonical_uri="/bucket/path/doc.txt",
        region="us-east-1",
        access_key="access",
        secret_key="secret",
    )

    assert headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=access/")
    assert "SignedHeaders=host;x-amz-content-sha256;x-amz-date" in headers["Authorization"]
    assert headers["x-amz-content-sha256"]
    assert headers["x-amz-date"]


def test_ragflow_enabled_requires_base_and_key() -> None:
    assert not Settings(RAGFLOW_API_BASE="http://ragflow:9380").ragflow_enabled
    assert not Settings(RAGFLOW_API_KEY="token").ragflow_enabled
    assert Settings(RAGFLOW_API_BASE="http://ragflow:9380", RAGFLOW_API_KEY="token").ragflow_enabled


async def test_ragflow_client_upload_parse_poll_flow(monkeypatch) -> None:
    calls: list[tuple[str, str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.url.query.decode()))
        if request.method == "GET" and request.url.path == "/api/v1/datasets":
            return httpx.Response(200, json={"code": 0, "data": []})
        if request.method == "POST" and request.url.path == "/api/v1/datasets":
            return httpx.Response(
                200, json={"code": 0, "data": {"id": "dataset-1", "name": "target"}}
            )
        if request.method == "POST" and request.url.path == "/api/v1/datasets/dataset-1/documents":
            return httpx.Response(
                200,
                json={"code": 0, "data": [{"id": "document-1", "name": "Inline-Smoke.txt"}]},
            )
        if (
            request.method == "POST"
            and request.url.path == "/api/v1/datasets/dataset-1/documents/parse"
        ):
            return httpx.Response(200, json={"code": 0})
        if request.method == "GET" and request.url.path == "/api/v1/datasets/dataset-1/documents":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "docs": [
                            {
                                "id": "document-1",
                                "name": "Inline-Smoke.txt",
                                "run": "DONE",
                                "chunk_count": 2,
                                "token_count": 8,
                            }
                        ],
                    },
                },
            )
        return httpx.Response(404, json={"code": 404, "message": "unexpected"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    def make_client(settings: Settings) -> RAGFlowClient:
        return RAGFlowClient(settings, client=client)

    monkeypatch.setattr("app.infrastructure.external.ragflow.RAGFlowClient", make_client)
    result = await ingest_into_ragflow(
        settings=Settings(
            RAGFLOW_API_BASE="http://ragflow:9380",
            RAGFLOW_API_KEY="token",
            RAGFLOW_PARSE_TIMEOUT_SECONDS=1,
        ),
        target_dataset="target",
        title="Inline Smoke",
        canonical_url=None,
        source_artifact_refs=[],
        metadata_json={"text": "hello"},
        source_document_version_id="version-1",
    )
    await client.aclose()

    assert result.dataset_id == "dataset-1"
    assert result.document_id == "document-1"
    assert result.parse_status == "DONE"
    assert result.chunk_count == 2
    assert calls == [
        ("GET", "/api/v1/datasets", "page_size=100"),
        ("POST", "/api/v1/datasets", ""),
        ("POST", "/api/v1/datasets/dataset-1/documents", ""),
        ("POST", "/api/v1/datasets/dataset-1/documents/parse", ""),
        ("GET", "/api/v1/datasets/dataset-1/documents", "id=document-1"),
    ]


def test_celery_delivery_options_use_platform_queue(monkeypatch) -> None:
    monkeypatch.setenv("CELERY_QUEUE", "knowledge.admin.default")
    from core.config import get_settings

    get_settings.cache_clear()
    options = CeleryProducer()._delivery_options()

    assert options == {
        "queue": "knowledge.admin.default",
        "exchange": "knowledge.admin.default",
        "routing_key": "knowledge.admin.default",
    }
    get_settings.cache_clear()
