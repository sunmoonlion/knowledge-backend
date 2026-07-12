from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import get_settings

from app.infrastructure.external.ragflow import (
    RAGFlowError,
    check_ragflow_config,
    ingest_into_ragflow,
    resolve_artifact_content,
)
from app.infrastructure.models.knowledge import KnowledgeIngestionJob
from app.interfaces.schemas.knowledge import KnowledgeIngestionCreate


TERMINAL_STATUSES = {
    "succeeded",
    "failed",
    "ragflow_config_error",
    "ragflow_parse_failed",
    "artifact_unreadable",
    "external_api_error",
    "artifact_verified",
}
RETRY_BLOCKED_STATUSES = {"ragflow_config_error"}
PROCESSOR_NAME = "artifact-contract-verifier"
RAGFLOW_PROCESSOR_NAME = "ragflow-ingestion-worker"


def build_idempotency_key(payload: KnowledgeIngestionCreate) -> str:
    return payload.idempotency_key


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
    result = payload.model_dump(mode="json", exclude_none=True)
    result["idempotency_key"] = idempotency_key
    return result


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
        target_dataset=payload.dataset_key,
        profile_key="markdown",
        idempotency_key=idempotency_key,
        title=payload.document.title,
        canonical_url=payload.document.canonical_url,
        source_name=payload.document.source_name,
        content_hash=payload.document.content_hash,
        source_artifact_refs=[payload.artifact.model_dump(mode="json")],
        metadata_json=payload.document.metadata,
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
    source_document_id: uuid.UUID | None,
    source_document_version_id: uuid.UUID | None,
    target_dataset: str | None,
    status: str | None,
    ragflow_document_id: str | None,
    idempotency_key: str | None,
    created_from: datetime | None,
    created_to: datetime | None,
    limit: int,
    offset: int,
) -> list[KnowledgeIngestionJob]:
    query = select(KnowledgeIngestionJob)
    if source_app:
        query = query.where(KnowledgeIngestionJob.source_app == source_app)
    if source_document_id:
        query = query.where(KnowledgeIngestionJob.source_document_id == source_document_id)
    if source_document_version_id:
        query = query.where(
            KnowledgeIngestionJob.source_document_version_id == source_document_version_id
        )
    if target_dataset:
        query = query.where(KnowledgeIngestionJob.target_dataset == target_dataset)
    if status:
        query = query.where(KnowledgeIngestionJob.status == status)
    if ragflow_document_id:
        query = query.where(KnowledgeIngestionJob.ragflow_document_id == ragflow_document_id)
    if idempotency_key:
        query = query.where(KnowledgeIngestionJob.idempotency_key == idempotency_key)
    if created_from:
        query = query.where(KnowledgeIngestionJob.created_at >= created_from)
    if created_to:
        query = query.where(KnowledgeIngestionJob.created_at <= created_to)
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


def classify_ingestion_error(exc: BaseException) -> tuple[str, dict[str, Any]]:
    message = str(exc)
    message_lower = message.lower()
    if any(
        marker in message_lower
        for marker in (
            "artifact",
            "s3 object",
            "s3 endpoint",
            "bucket",
            "object key",
            "sha256",
            "content type",
            "storage version",
        )
    ):
        return "artifact_unreadable", {"error_type": "artifact_unreadable"}
    if "no default embedding model" in message_lower:
        return "ragflow_config_error", {"error_type": "ragflow_config_error"}
    if "parse timed out" in message_lower or "parse failed" in message_lower:
        return "ragflow_parse_failed", {"error_type": "ragflow_parse_failed"}
    if isinstance(exc, RAGFlowError):
        return "external_api_error", {"error_type": "external_api_error", "system": "ragflow"}
    if isinstance(exc, OSError):
        return "external_api_error", {"error_type": "external_api_error"}
    return "failed", {"error_type": "unknown"}


async def retry_ingestion_job(
    session: AsyncSession, *, ingestion_id: uuid.UUID, force: bool = False, reason: str | None = None
) -> KnowledgeIngestionJob:
    job = await get_ingestion_job(session, ingestion_id)
    if job is None:
        raise ValueError(f"ingestion job not found: {ingestion_id}")
    if job.status not in TERMINAL_STATUSES:
        raise ValueError(f"ingestion job is not terminal: {job.status}")
    if job.status == "succeeded":
        raise ValueError("succeeded ingestion job cannot be retried")
    if job.status in RETRY_BLOCKED_STATUSES and not force:
        raise ValueError(f"retry blocked for {job.status}; fix configuration first or force retry")

    metadata_json = dict(job.metadata_json or {})
    retry_history = list(metadata_json.get("retry_history") or [])
    retry_count = int(metadata_json.get("retry_count") or 0) + 1
    retry_entry = {
        "attempt": retry_count,
        "at": datetime.now(UTC).isoformat(),
        "from_status": job.status,
        "force": force,
    }
    if reason:
        retry_entry["reason"] = reason
    retry_history.append(retry_entry)
    metadata_json["retry_count"] = retry_count
    metadata_json["retry_history"] = retry_history
    metadata_json["last_retry_at"] = retry_entry["at"]

    history = list(job.status_history or [])
    history.append(
        _status_entry(
            "accepted",
            metadata={
                "retry": True,
                "attempt": retry_count,
                "from_status": job.status,
                "force": force,
                **({"reason": reason} if reason else {}),
            },
        )
    )
    job.status = "accepted"
    job.last_error = None
    job.completed_at = None
    job.status_history = history
    job.metadata_json = metadata_json
    await session.commit()
    await session.refresh(job)
    return job


async def get_ragflow_config_check() -> dict[str, Any]:
    result = await check_ragflow_config(get_settings())
    return {
        "enabled": result.enabled,
        "reachable": result.reachable,
        "has_default_embedding": result.has_default_embedding,
        "ready": result.enabled and result.reachable and result.has_default_embedding,
        "issues": result.issues,
        "details": result.details,
    }


def build_processing_success_metadata(job: KnowledgeIngestionJob) -> dict[str, Any]:
    return {
        "processor": PROCESSOR_NAME,
        "mode": "artifact_verification",
        "artifact_ref_count": len(job.source_artifact_refs or []),
        "ragflow": "not_requested",
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
            failure_status, failure_metadata = classify_ingestion_error(exc)
            return await update_ingestion_status(
                session,
                ingestion_id=ingestion_id,
                status=failure_status,
                last_error=str(exc),
                metadata={
                    "processor": RAGFLOW_PROCESSOR_NAME,
                    "mode": "ragflow",
                    **failure_metadata,
                },
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

    try:
        artifact = await resolve_artifact_content(
            settings=settings,
            source_artifact_refs=list(job.source_artifact_refs or []),
            title=job.title,
            canonical_url=job.canonical_url,
            metadata_json=dict(job.metadata_json or {}),
            source_document_version_id=str(job.source_document_version_id),
        )
    except (RAGFlowError, OSError, ValueError) as exc:
        failure_status, failure_metadata = classify_ingestion_error(exc)
        return await update_ingestion_status(
            session,
            ingestion_id=ingestion_id,
            status=failure_status,
            last_error=str(exc),
            metadata={"processor": PROCESSOR_NAME, **failure_metadata},
            knowledge_document_id=None,
            ragflow_document_id=None,
        )
    return await update_ingestion_status(
        session,
        ingestion_id=ingestion_id,
        status="artifact_verified",
        last_error=None,
        metadata={
            **build_processing_success_metadata(job),
            "verified_size_bytes": len(artifact.content),
            "verified_content_type": artifact.content_type,
        },
        knowledge_document_id=None,
        ragflow_document_id=None,
    )
