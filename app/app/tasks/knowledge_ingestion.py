from __future__ import annotations

from app.worker import celery_app


@celery_app.task(name="app.tasks.process_knowledge_ingestion")
def process_knowledge_ingestion(ingestion_id: str) -> str:
    """Old queued tasks must fail closed after the durable-delivery cutover."""
    raise RuntimeError(
        "legacy Knowledge task disabled; use the durable ingestion request"
    )
