from __future__ import annotations

import asyncio
import uuid

from app.application.services.knowledge_ingestion_service import process_ingestion_job
from app.infrastructure.storage.postgres import get_postgres
from app.worker import celery_app


@celery_app.task(name="app.tasks.process_knowledge_ingestion")
def process_knowledge_ingestion(ingestion_id: str) -> str:
    """Process one knowledge ingestion job through the M1 mock worker."""
    return asyncio.run(_run(uuid.UUID(ingestion_id)))


async def _run(ingestion_id: uuid.UUID) -> str:
    postgres = get_postgres()
    await postgres.init()
    try:
        async with postgres.session_factory() as session:
            job = await process_ingestion_job(session, ingestion_id=ingestion_id)
            return str(job.id)
    finally:
        await postgres.shutdown()
