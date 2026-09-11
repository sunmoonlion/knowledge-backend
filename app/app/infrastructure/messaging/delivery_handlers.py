"""Knowledge domain extension for the shared durable task runtime."""

import uuid

from app.application.services.durable_tasks import Handler


async def ingest(session, payload):
    from app.application.services.knowledge_ingestion_service import (
        process_ingestion_job,
    )

    await process_ingestion_job(
        session, ingestion_id=uuid.UUID(payload["ingestion_id"])
    )


def get_delivery_handlers() -> dict[str, Handler]:
    return {"knowledge.ingest.v1": ingest}
