"""Recover provider receipts without repeating an unacknowledged remote write."""

from __future__ import annotations

import hashlib
import uuid
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.services.durable_tasks import assert_execution_current
from app.application.services.ingestion_authorization import (
    require_job_binding,
    resolve_binding,
)
from app.infrastructure.external.ragflow import (
    ArtifactContent,
    RAGFlowClient,
    RAGFlowError,
    RAGFlowIngestionResult,
    RAGFlowProtocolError,
    _maybe_int,
    _normalise_run,
    _wait_for_document_parse,
    resolve_artifact_content,
)
from app.infrastructure.models.knowledge import (
    KnowledgeIngestionJob,
    KnowledgeProviderOperation,
)
from core.config import Settings


class RAGFlowOutcomeUnknown(RAGFlowError):
    """Reconcile or explicitly investigate; absence never permits another write."""


async def operation(
    session: AsyncSession, key: str, intent: dict
) -> KnowledgeProviderOperation:
    await session.execute(
        insert(KnowledgeProviderOperation)
        .values(
            operation_key=key,
            intent=intent,
            state="intent",
            receipt={},
        )
        .on_conflict_do_nothing(index_elements=["operation_key"])
    )
    row = (
        await session.execute(
            select(KnowledgeProviderOperation)
            .where(KnowledgeProviderOperation.operation_key == key)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    if row.state == "legacy_unknown":
        raise RAGFlowOutcomeUnknown(
            "legacy running ingestion requires receipt investigation"
        )
    if row.intent != intent:
        raise RAGFlowProtocolError("provider operation intent or tenant changed")
    await session.commit()
    return row


async def start(session: AsyncSession, key: str) -> bool:
    await assert_execution_current(session)
    claimed = (
        await session.execute(
            update(KnowledgeProviderOperation)
            .where(
                KnowledgeProviderOperation.operation_key == key,
                KnowledgeProviderOperation.state == "intent",
            )
            .values(state="executing")
            .returning(KnowledgeProviderOperation.operation_key)
        )
    ).scalar_one_or_none()
    await session.commit()  # Persist uncertainty BEFORE the network write.
    return claimed is not None


async def confirm(session: AsyncSession, key: str, receipt: dict) -> None:
    row = (
        await session.execute(
            select(KnowledgeProviderOperation)
            .where(KnowledgeProviderOperation.operation_key == key)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    if row.state == "confirmed" and row.receipt != receipt:
        raise RAGFlowProtocolError("provider receipt conflicts with confirmed identity")
    row.state, row.receipt = "confirmed", receipt
    await session.commit()  # Shared consumer guards fence every intermediate commit.


async def unknown(session: AsyncSession, key: str) -> None:
    await session.execute(
        update(KnowledgeProviderOperation)
        .where(
            KnowledgeProviderOperation.operation_key == key,
            KnowledgeProviderOperation.state == "executing",
        )
        .values(state="unknown")
    )
    await session.commit()


async def dataset(
    session: AsyncSession,
    client: RAGFlowClient,
    name: str,
    scope: str,
    settings: Settings,
) -> dict:
    binding = resolve_binding(settings, name)
    key = f"dataset:{name}"
    await operation(session, key, {"name": name, "provider_scope": scope})
    matches = await client.find_datasets(binding.dataset_name)
    if len(matches) > 1:
        raise RAGFlowOutcomeUnknown("multiple datasets match the intended identity")
    if not matches:
        raise RAGFlowProtocolError(
            "configured dataset is not accessible; "
            "provisioning is not a data-plane operation"
        )
    result = matches[0]
    if (
        result.get("name") != binding.dataset_name
        or result.get("id") != binding.dataset_id
    ):
        raise RAGFlowProtocolError("dataset receipt does not match intended identity")
    await confirm(session, key, {"id": result["id"], "name": name})
    return result


async def uploaded_document(
    session: AsyncSession,
    client: RAGFlowClient,
    *,
    key: str,
    intent: dict,
    artifact: ArtifactContent,
    recovery_document_id: str | None = None,
) -> dict:
    row = await operation(session, key, intent)
    dataset_id = intent["dataset_id"]
    if recovery_document_id is not None:
        matches = [await client.get_document(dataset_id, recovery_document_id)]
    elif row.state == "confirmed":
        matches = [await client.get_document(dataset_id, row.receipt["document_id"])]
    else:
        matches = await client.find_documents(dataset_id, artifact.filename)
    if len(matches) > 1:
        raise RAGFlowOutcomeUnknown("multiple documents match the upload identity")
    if not matches:
        if not await start(session, key):
            raise RAGFlowOutcomeUnknown(
                "upload outcome has no verified receipt; not uploading again"
            )
        try:
            await assert_execution_current(session)
            matches = [await client.upload_document(dataset_id, artifact)]
        except RAGFlowError:
            await unknown(session, key)
            raise RAGFlowOutcomeUnknown(
                "upload outcome requires reconciliation"
            ) from None
    document = matches[0]
    if recovery_document_id is not None and document.get("id") != recovery_document_id:
        raise RAGFlowProtocolError("recovery returned another document identity")
    if (
        document.get("name") != artifact.filename
        or document.get("dataset_id") != dataset_id
        or not isinstance(document.get("id"), str)
        or not document["id"]
    ):
        raise RAGFlowProtocolError(
            "upload receipt does not match source version and dataset"
        )
    await client.verify_document_content(
        dataset_id, document["id"], size=len(artifact.content), sha256=intent["sha256"]
    )
    await confirm(
        session,
        key,
        {
            "dataset_id": dataset_id,
            "document_id": document["id"],
            "filename": artifact.filename,
            "sha256": intent["sha256"],
        },
    )
    return document


def upload_identity(job: KnowledgeIngestionJob) -> uuid.UUID:
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"knowledge-upload:{job.source_app}:{job.source_document_version_id}:"
        f"{job.target_dataset or 'default'}",
    )


async def prepare_artifact(
    job: KnowledgeIngestionJob, settings: Settings
) -> ArtifactContent:
    artifact = await resolve_artifact_content(
        settings=settings,
        title=job.title,
        canonical_url=job.canonical_url,
        source_artifact_refs=list(job.source_artifact_refs or []),
        metadata_json=dict(job.metadata_json or {}),
        source_document_version_id=str(job.source_document_version_id),
    )
    checksum = hashlib.sha256(artifact.content).hexdigest()
    extension = "md" if artifact.content_type.startswith("text/markdown") else "txt"
    return ArtifactContent(
        f"knowledge-{upload_identity(job).hex}-{checksum}.{extension}",
        artifact.content,
        artifact.content_type,
    )


async def provider_scope(client: RAGFlowClient, settings: Settings) -> str:
    tenant_id = (await client.get_tenant_models()).get("tenant_id")
    if not isinstance(tenant_id, str) or not tenant_id:
        raise RAGFlowProtocolError("provider tenant identity is missing")
    provider_base = settings.ragflow_api_base
    if not provider_base:
        raise RAGFlowProtocolError("provider endpoint is missing")
    return hashlib.sha256(
        f"{provider_base.rstrip('/')}|{tenant_id}".encode()
    ).hexdigest()


def upload_intent(
    job: KnowledgeIngestionJob, scope: str, dataset_id: str, artifact: ArtifactContent
) -> dict:
    return {
        "provider_scope": scope,
        "dataset_id": dataset_id,
        "source_app": job.source_app,
        "source_version": str(job.source_document_version_id),
        "artifact_refs": job.source_artifact_refs,
        "sha256": hashlib.sha256(artifact.content).hexdigest(),
        "filename": artifact.filename,
    }


async def recover_upload_receipt(
    session: AsyncSession,
    *,
    job: KnowledgeIngestionJob,
    settings: Settings,
    document_id: str,
) -> dict:
    """Operator-selected receipt, verified using GETs only; no upload or parse."""
    binding = require_job_binding(job, settings)
    key = f"upload:{upload_identity(job)}"
    row = await session.get(KnowledgeProviderOperation, key)
    if row is None or row.state == "legacy_unknown":
        raise RAGFlowOutcomeUnknown(
            "no stable upload intent; manual legacy investigation required"
        )
    if row.intent.get("dataset_id") != binding.dataset_id:
        raise RAGFlowProtocolError("recovery dataset differs from authorized binding")
    artifact = await prepare_artifact(job, settings)
    client = RAGFlowClient(settings)
    try:
        scope = await provider_scope(client, settings)
        expected = upload_intent(job, scope, row.intent["dataset_id"], artifact)
        if expected != row.intent:
            raise RAGFlowProtocolError(
                "recovery source version or provider scope changed"
            )
        return await uploaded_document(
            session,
            client,
            key=key,
            intent=expected,
            artifact=artifact,
            recovery_document_id=document_id,
        )
    finally:
        await client.close()


async def ingest_with_receipts(
    session: AsyncSession, *, job: KnowledgeIngestionJob, settings: Settings
) -> RAGFlowIngestionResult:
    # Direct/old task entrypoints must not bypass the consumer fencing context.
    if "delivery_lease" not in session.info:
        raise RuntimeError("RAGFlow ingestion requires the durable consumer lease")
    require_job_binding(job, settings)
    identity = upload_identity(job)
    key = f"upload:{identity}"
    legacy = await session.get(KnowledgeProviderOperation, key)
    if legacy is not None and legacy.state == "legacy_unknown":
        raise RAGFlowOutcomeUnknown(
            "legacy running ingestion requires receipt investigation"
        )
    artifact = await prepare_artifact(job, settings)
    client = RAGFlowClient(settings)
    try:
        scope = await provider_scope(client, settings)
        target = await dataset(
            session, client, job.target_dataset or "", scope, settings
        )
        intent = upload_intent(job, scope, target["id"], artifact)
        document = await uploaded_document(
            session, client, key=key, intent=intent, artifact=artifact
        )
        generation = int((job.metadata_json or {}).get("retry_count") or 0)
        parse_key = f"parse:{identity}:{generation}"
        await operation(
            session,
            parse_key,
            {"dataset_id": target["id"], "document_id": document["id"]},
        )
        current = await client.get_document(target["id"], document["id"])
        run = _normalise_run(current.get("run"))
        if run not in {"UNSTART", "FAIL", "CANCEL", "DONE", "RUNNING", "SCHEDULE"}:
            raise RAGFlowOutcomeUnknown("unrecognised parse state")
        if run not in {"DONE", "RUNNING", "SCHEDULE"}:
            unfinished = (
                await session.execute(
                    select(KnowledgeProviderOperation.operation_key)
                    .where(
                        KnowledgeProviderOperation.operation_key.startswith(
                            f"parse:{identity}:"
                        ),
                        KnowledgeProviderOperation.state.in_(["executing", "unknown"]),
                    )
                    .limit(1)
                )
            ).first()
            if unfinished is not None:
                raise RAGFlowOutcomeUnknown(
                    "prior parse submission is still unconfirmed"
                )
            if not await start(session, parse_key):
                raise RAGFlowOutcomeUnknown("parse outcome requires reconciliation")
            try:
                await assert_execution_current(session)
                await client.parse_document(target["id"], document["id"])
                await confirm(
                    session,
                    parse_key,
                    {
                        "document_id": document["id"],
                        "submitted": True,
                    },
                )
            except RAGFlowError:
                await unknown(session, parse_key)
                raise RAGFlowOutcomeUnknown(
                    "parse outcome requires reconciliation"
                ) from None
        final = await _wait_for_document_parse(
            client=client,
            dataset_id=target["id"],
            document_id=document["id"],
            timeout_seconds=settings.ragflow_parse_timeout_seconds,
            interval_seconds=settings.ragflow_parse_poll_interval_seconds,
        )
        await confirm(
            session, parse_key, {"document_id": document["id"], "submitted": True}
        )
        metadata: dict[str, Any] = {
            "ragflow_dataset_id": target["id"],
            "ragflow_dataset_name": target["name"],
            "ragflow_document_name": artifact.filename,
            "ragflow_parse_status": str(final.get("run") or ""),
            "ragflow_chunk_count": _maybe_int(final.get("chunk_count")),
            "ragflow_token_count": _maybe_int(final.get("token_count")),
        }
        return RAGFlowIngestionResult(
            target["id"],
            target["name"],
            document["id"],
            artifact.filename,
            str(final.get("run") or ""),
            _maybe_int(final.get("chunk_count")),
            _maybe_int(final.get("token_count")),
            metadata,
        )
    finally:
        await client.close()
