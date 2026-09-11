"""Inspect provider intents or verify an operator-selected upload receipt.

Recovery only issues GET requests to RAGFlow; it neither uploads, parses, marks
the domain job succeeded, nor automatically replays a delivery command.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import uuid

from sqlalchemy import select

from app.application.services.ragflow_delivery import (
    recover_upload_receipt,
    upload_identity,
)
from app.infrastructure.models.knowledge import (
    KnowledgeIngestionJob,
    KnowledgeProviderOperation,
)
from app.infrastructure.storage.postgres import get_postgres
from core.config import get_settings


async def run(args):
    postgres = get_postgres()
    await postgres.init()
    try:
        async with postgres.session_factory() as session:
            job = await session.get(KnowledgeIngestionJob, uuid.UUID(args.ingestion_id))
            if job is None:
                raise ValueError("ingestion job not found")
            if args.command == "recover-upload":
                if not args.document_id:
                    raise ValueError("--document-id is required")
                document = await recover_upload_receipt(
                    session,
                    job=job,
                    settings=get_settings(),
                    document_id=args.document_id,
                )
                return {
                    "verified": True,
                    "ingestion_id": str(job.id),
                    "document_id": document["id"],
                    "replayed": False,
                }
            identity = upload_identity(job)
            rows = await session.execute(
                select(KnowledgeProviderOperation).where(
                    (
                        KnowledgeProviderOperation.operation_key
                        == f"dataset:{job.target_dataset or 'default'}"
                    )
                    | (KnowledgeProviderOperation.operation_key == f"upload:{identity}")
                    | KnowledgeProviderOperation.operation_key.startswith(
                        f"parse:{identity}:"
                    )
                )
            )
            return [
                {
                    "operation_key": row.operation_key,
                    "state": row.state,
                    "receipt": row.receipt,
                    "updated_at": row.updated_at,
                }
                for row in rows.scalars()
            ]
    finally:
        await postgres.shutdown()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["show", "recover-upload"])
    parser.add_argument("--ingestion-id", required=True)
    parser.add_argument("--document-id")
    print(json.dumps(asyncio.run(run(parser.parse_args())), default=str))


if __name__ == "__main__":
    main()
