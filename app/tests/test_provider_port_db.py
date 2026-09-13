"""A non-RAGFlow test double drives the real journal and durable consumer.

This is an internal Port proof, not an installed WeKnora or external-v1 claim.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import replace

import pytest
from test_durable_delivery_db import db as db
from test_durable_delivery_db import sql
from test_knowledge_delivery_db import CONTENT, authorized_settings, message, submit

from app.application.ports.knowledge_provider import (
    ArtifactContent,
    ParseStatus,
    ProviderDataset,
    ProviderDocument,
    ProviderError,
    ProviderOutcomeUnknown,
    ProviderProtocolError,
    ProviderRetrievalResult,
)
from app.application.services import knowledge_ingestion_service as service
from app.application.services import provider_delivery as delivery
from app.application.services.durable_tasks import DurableTasks
from app.application.services.ingestion_execution import guard_generation
from app.infrastructure.models.knowledge import KnowledgeIngestionJob


class MemoryIndex:
    name = "test-index"

    def __init__(self, lose_upload=False):
        self.lose_upload = lose_upload
        self.document = None
        self.uploads = self.parses = self.closes = 0
        self.scope_value = "test-index:endpoint:tenant"

    async def close(self):
        self.closes += 1

    async def scope(self):
        return self.scope_value

    async def find_datasets(self, name):
        return [ProviderDataset("dataset-1", name)]

    async def find_documents(self, dataset_id, filename):
        return [self.document] if self.document else []

    async def get_document(self, dataset_id, document_id):
        assert (self.document.dataset_id, self.document.id) == (dataset_id, document_id)
        return self.document

    async def upload_document(self, dataset_id, artifact):
        self.uploads += 1
        self.document = ProviderDocument(
            "neutral-document", dataset_id, artifact.filename, ParseStatus.NOT_STARTED
        )
        if self.lose_upload:
            self.lose_upload = False
            raise ProviderError("response lost after remote commit")
        return self.document

    async def verify_document_content(self, dataset_id, document_id, *, size, sha256):
        assert size == len(CONTENT)
        assert sha256 == hashlib.sha256(CONTENT).hexdigest()

    async def parse_document(self, dataset_id, document_id):
        self.parses += 1
        self.document = replace(self.document, parse_status=ParseStatus.SUCCEEDED)

    def ingestion_metadata(self, dataset, document):
        return {"index": "test-index"}

    async def retrieve(self, *, question, dataset_ids, document_ids, top_k):
        return ProviderRetrievalResult([], 0)


def configure(monkeypatch, index):
    monkeypatch.setattr(service, "get_settings", authorized_settings)
    monkeypatch.setattr(delivery, "create_provider", lambda _: index)

    async def artifact(**unused):
        return ArtifactContent("clean.md", CONTENT, "text/markdown")

    monkeypatch.setattr(delivery, "resolve_artifact_content", artifact)


def runtime(db, results):
    async def handler(session, payload):
        job = await session.get(
            KnowledgeIngestionJob, uuid.UUID(payload["ingestion_id"])
        )
        guard_generation(session, job, payload["generation"])
        result = await delivery.ingest_with_receipts(
            session, job=job, settings=authorized_settings()
        )
        results.append(result)

    return DurableTasks(db, handlers={"knowledge.ingest.v1": handler})


async def test_non_ragflow_port_completes_journal_and_duplicate_inbox(db, monkeypatch):
    index, results = MemoryIndex(), []
    configure(monkeypatch, index)
    await submit(db)
    consumer, mid = runtime(db, results), await message(db)
    assert await consumer.consume(mid)
    assert await consumer.consume(mid) is False
    assert results[0].provider == "test-index"
    assert results[0].parse_status == ParseStatus.SUCCEEDED
    assert results[0].metadata == {"index": "test-index"}
    assert (index.uploads, index.parses, index.closes) == (1, 1, 1)
    assert await sql(db, "SELECT count(*) FROM inbox_message") == 1
    assert (
        await sql(
            db,
            "SELECT count(*) FROM knowledge_provider_operation WHERE state='confirmed'",
        )
        == 3
    )
    # The legacy external projection is intentionally NOT invoked by this test handler.
    assert await sql(db, "SELECT count(*) FROM knowledge_document_version") == 0


async def test_neutral_provider_unknown_write_recovers_without_upload_retry(
    db, monkeypatch
):
    index, results = MemoryIndex(lose_upload=True), []
    configure(monkeypatch, index)
    job_id = await submit(db)
    consumer, mid = runtime(db, results), await message(db)
    with pytest.raises(ProviderOutcomeUnknown):
        await consumer.consume(mid)
    assert await sql(db, "SELECT count(*) FROM inbox_message") == 0
    async with db() as session:
        job = await session.get(KnowledgeIngestionJob, job_id)
        document = await delivery.recover_upload_receipt(
            session,
            job=job,
            settings=authorized_settings(),
            document_id=index.document.id,
        )
        assert document == index.document
    assert index.uploads == 1 and index.parses == 0
    assert await consumer.consume(mid)
    assert index.uploads == 1 and index.parses == 1
    assert await sql(db, "SELECT count(*) FROM inbox_message") == 1


async def test_scope_change_rejects_old_receipt_without_new_write(db, monkeypatch):
    index, results = MemoryIndex(lose_upload=True), []
    configure(monkeypatch, index)
    job_id = await submit(db)
    with pytest.raises(ProviderOutcomeUnknown):
        await runtime(db, results).consume(await message(db))
    index.scope_value = "other-provider-or-tenant"
    async with db() as session:
        job = await session.get(KnowledgeIngestionJob, job_id)
        with pytest.raises(ProviderProtocolError, match="scope changed"):
            await delivery.recover_upload_receipt(
                session,
                job=job,
                settings=authorized_settings(),
                document_id=index.document.id,
            )
    assert index.uploads == 1 and index.parses == 0
    assert await sql(db, "SELECT count(*) FROM inbox_message") == 0
