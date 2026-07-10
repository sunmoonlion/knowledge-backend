from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.services import knowledge_ingestion_service
from app.infrastructure.messaging.celery_producer import get_celery_producer
from app.infrastructure.storage.postgres import get_db_session
from app.interfaces.schemas.knowledge import (
    KnowledgeIngestionCreate,
    KnowledgeIngestionRead,
    KnowledgeIngestionStatusUpdate,
)

router = APIRouter(prefix="/knowledge", tags=["知识入库"])


@router.post(
    "/ingestions",
    response_model=KnowledgeIngestionRead,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_ingestion(
    payload: KnowledgeIngestionCreate,
    session: AsyncSession = Depends(get_db_session),
):
    job = await knowledge_ingestion_service.submit_ingestion(session, payload)
    producer = get_celery_producer()
    if job.status == "accepted" and producer.enabled:
        producer.dispatch_knowledge_ingestion(job.id)
    return job


@router.get("/ingestions", response_model=list[KnowledgeIngestionRead])
async def list_ingestions(
    source_app: str | None = Query(default=None),
    source_document_version_id: uuid.UUID | None = Query(default=None),
    ingestion_status: str | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_db_session),
):
    return await knowledge_ingestion_service.list_ingestion_jobs(
        session,
        source_app=source_app,
        source_document_version_id=source_document_version_id,
        status=ingestion_status,
        limit=limit,
        offset=offset,
    )


@router.get("/ingestions/{ingestion_id}", response_model=KnowledgeIngestionRead)
async def get_ingestion(
    ingestion_id: uuid.UUID, session: AsyncSession = Depends(get_db_session)
):
    job = await knowledge_ingestion_service.get_ingestion_job(session, ingestion_id)
    if job is None:
        raise HTTPException(status_code=404, detail="ingestion job not found")
    return job


@router.post("/ingestions/{ingestion_id}/status", response_model=KnowledgeIngestionRead)
async def update_ingestion_status(
    ingestion_id: uuid.UUID,
    payload: KnowledgeIngestionStatusUpdate,
    session: AsyncSession = Depends(get_db_session),
):
    try:
        return await knowledge_ingestion_service.update_ingestion_status(
            session,
            ingestion_id=ingestion_id,
            status=payload.status,
            last_error=payload.last_error,
            metadata=payload.metadata,
            knowledge_document_id=payload.knowledge_document_id,
            ragflow_document_id=payload.ragflow_document_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/ingestions/{ingestion_id}/dispatch", response_model=KnowledgeIngestionRead)
async def dispatch_ingestion(
    ingestion_id: uuid.UUID, session: AsyncSession = Depends(get_db_session)
):
    job = await knowledge_ingestion_service.get_ingestion_job(session, ingestion_id)
    if job is None:
        raise HTTPException(status_code=404, detail="ingestion job not found")

    producer = get_celery_producer()
    if producer.enabled:
        producer.dispatch_knowledge_ingestion(ingestion_id)
        return job

    try:
        return await knowledge_ingestion_service.process_ingestion_job(
            session, ingestion_id=ingestion_id
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
