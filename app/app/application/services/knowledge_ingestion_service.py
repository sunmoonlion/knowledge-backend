from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import get_settings

from app.infrastructure.external.ragflow import RAGFlowError, ingest_into_ragflow
from app.infrastructure.models.knowledge import KnowledgeIngestionJob
from app.interfaces.schemas.knowledge import KnowledgeIngestionCreate


TERMINAL_STATUSES = {"succeeded", "failed"}
PROCESSOR_NAME = "mock-ingestion-worker"
RAGFLOW_PROCESSOR_NAME = "ragflow-ingestion-worker"


def build_idempotency_key(payload: KnowledgeIngestionCreate) -> str:
    if payload.idempotency_key:
        return payload.idempotency_key
    dataset = payload.target_dataset or "default"
    return f"{payload.source_app}:{payload.source_document_version_id}:{dataset}"


def _status_entry(
    status: str, *, last_error: str | None = None, metadata: dict[str, Any] | None = None
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "status": status,
        "at": datetime.now(UTC).isoformat(),
    }
    if last_error:
        entry["last_error"] = last_error
    if metadata:
        entry["metadata"] = metadata
    return entry


def _payload_dict(payload: KnowledgeIngestionCreate, idempotency_key: str) -> dict[str, Any]:
    return {
        "source_app": payload.source_app,
        "source_document_id": str(payload.source_document_id),
        "source_document_version_id": str(payload.source_document_version_id),
        "source_artifact_refs": [
            item.model_dump(exclude_none=True) for item in payload.source_artifact_refs
        ],
        "title": payload.title,
        "canonical_url": payload.canonical_url,
        "source_name": payload.source_name,
        "content_hash": payload.content_hash,
        "metadata": payload.metadata,
        "target_dataset": payload.target_dataset,
        "profile_key": payload.profile_key,
        "idempotency_key": idempotency_key,
    }


async def submit_ingestion(
    session: AsyncSession, payload: KnowledgeIngestionCreate
) -> KnowledgeIngestionJob:
    idempotency_key = build_idempotency_key(payload)
    existing = await get_ingestion_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return existing

    job = KnowledgeIngestionJob(
        source_app=payload.source_app,
        source_document_id=payload.source_document_id,
        source_document_version_id=payload.source_document_version_id,
        target_dataset=payload.target_dataset,
        profile_key=payload.profile_key,
        idempotency_key=idempotency_key,
        title=payload.title,
        canonical_url=payload.canonical_url,
        source_name=payload.source_name,
        content_hash=payload.content_hash,
        source_artifact_refs=[
            item.model_dump(exclude_none=True) for item in payload.source_artifact_refs
        ],
        metadata_json=payload.metadata,
        payload=_payload_dict(payload, idempotency_key),
        status="accepted",
        status_history=[_status_entry("accepted")],
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def get_ingestion_job(
    session: AsyncSession, ingestion_id: uuid.UUID
) -> KnowledgeIngestionJob | None:
    return await session.get(KnowledgeIngestionJob, ingestion_id)


async def get_ingestion_by_idempotency_key(
    session: AsyncSession, idempotency_key: str
) -> KnowledgeIngestionJob | None:
    result = await session.execute(
        select(KnowledgeIngestionJob).where(
            KnowledgeIngestionJob.idempotency_key == idempotency_key
        )
    )
    return result.scalar_one_or_none()


async def list_ingestion_jobs(
    session: AsyncSession,
    *,
    source_app: str | None,
    source_document_version_id: uuid.UUID | None,
    status: str | None,
    limit: int,
    offset: int,
) -> list[KnowledgeIngestionJob]:
    query = select(KnowledgeIngestionJob)
    if source_app:
        query = query.where(KnowledgeIngestionJob.source_app == source_app)
    if source_document_version_id:
        query = query.where(
            KnowledgeIngestionJob.source_document_version_id == source_document_version_id
        )
    if status:
        query = query.where(KnowledgeIngestionJob.status == status)
    query = query.order_by(KnowledgeIngestionJob.created_at.desc()).limit(limit).offset(offset)
    result = await session.execute(query)
    return list(result.scalars().all())


async def update_ingestion_status(
    session: AsyncSession,
    *,
    ingestion_id: uuid.UUID,
    status: str,
    last_error: str | None,
    metadata: dict,
    knowledge_document_id: str | None,
    ragflow_document_id: str | None,
) -> KnowledgeIngestionJob:
    job = await get_ingestion_job(session, ingestion_id)
    if job is None:
        raise ValueError(f"ingestion job not found: {ingestion_id}")

    history = list(job.status_history or [])
    history.append(_status_entry(status, last_error=last_error, metadata=metadata))
    job.status = status
    job.last_error = last_error
    job.status_history = history
    if knowledge_document_id is not None:
        job.knowledge_document_id = knowledge_document_id
    if ragflow_document_id is not None:
        job.ragflow_document_id = ragflow_document_id
    if status in TERMINAL_STATUSES:
        job.completed_at = datetime.now(UTC)

    await session.commit()
    await session.refresh(job)
    return job


def build_processing_success_metadata(job: KnowledgeIngestionJob) -> dict[str, Any]:
    return {
        "processor": PROCESSOR_NAME,
        "mode": "mock",
        "artifact_ref_count": len(job.source_artifact_refs or []),
        "ragflow": "deferred",
    }


def build_ragflow_success_metadata(
    job: KnowledgeIngestionJob, ragflow_metadata: dict[str, Any]
) -> dict[str, Any]:
    return {
        "processor": RAGFLOW_PROCESSOR_NAME,
        "mode": "ragflow",
        "artifact_ref_count": len(job.source_artifact_refs or []),
        "ragflow": "processed",
        **ragflow_metadata,
    }


async def process_ingestion_job(
    session: AsyncSession, *, ingestion_id: uuid.UUID
) -> KnowledgeIngestionJob:
    job = await get_ingestion_job(session, ingestion_id)
    if job is None:
        raise ValueError(f"ingestion job not found: {ingestion_id}")
    if job.status in TERMINAL_STATUSES:
        return job

    settings = get_settings()
    processor_name = RAGFLOW_PROCESSOR_NAME if settings.ragflow_enabled else PROCESSOR_NAME
    job = await update_ingestion_status(
        session,
        ingestion_id=ingestion_id,
        status="running",
        last_error=None,
        metadata={"processor": processor_name},
        knowledge_document_id=None,
        ragflow_document_id=None,
    )

    if settings.ragflow_enabled:
        try:
            result = await ingest_into_ragflow(
                settings=settings,
                target_dataset=job.target_dataset or "default",
                title=job.title,
                canonical_url=job.canonical_url,
                source_artifact_refs=list(job.source_artifact_refs or []),
                metadata_json=dict(job.metadata_json or {}),
                source_document_version_id=str(job.source_document_version_id),
            )
        except (RAGFlowError, OSError, ValueError) as exc:
            return await update_ingestion_status(
                session,
                ingestion_id=ingestion_id,
                status="failed",
                last_error=str(exc),
                metadata={"processor": RAGFLOW_PROCESSOR_NAME, "mode": "ragflow"},
                knowledge_document_id=None,
                ragflow_document_id=None,
            )
        return await update_ingestion_status(
            session,
            ingestion_id=ingestion_id,
            status="succeeded",
            last_error=None,
            metadata=build_ragflow_success_metadata(job, result.metadata),
            knowledge_document_id=f"ragflow-dataset:{result.dataset_id}",
            ragflow_document_id=result.document_id,
        )

    return await update_ingestion_status(
        session,
        ingestion_id=ingestion_id,
        status="succeeded",
        last_error=None,
        metadata=build_processing_success_metadata(job),
        knowledge_document_id=f"mock-knowledge-doc:{job.id}",
        ragflow_document_id=None,
    )
