"""Knowledge domain extension for the shared durable task runtime."""

import uuid

from app.application.services.durable_tasks import Handler


async def ingest(session, payload):
    from app.application.services.knowledge_ingestion_service import (
        process_ingestion_job,
    )

    generation, step = payload.get("generation"), payload.get("step")
    if (
        type(generation) is not int
        or type(step) is not int
        or generation < 0
        or step < 0
    ):
        raise ValueError("legacy or invalid ingestion command requires investigation")
    await process_ingestion_job(
        session,
        ingestion_id=uuid.UUID(payload["ingestion_id"]),
        generation=generation,
        step=step,
    )


def get_delivery_handlers() -> dict[str, Handler]:
    return {"knowledge.ingest.v1": ingest}
