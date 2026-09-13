"""Server-owned ingestion cursor, stored only on the owning job.

The acceptance marker distinguishes our state from legacy client metadata. Neither
client retry_count nor status-update metadata grants authority to advance a run.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.errors.exceptions import ForbiddenError
from app.infrastructure.messaging.durable_delivery import DeliveryLeaseLost
from app.infrastructure.models.knowledge import KnowledgeIngestionJob

EXECUTION_KEY = "ingestion_execution_v1"
EXECUTION_MARKER = {"version": 1}


class VerifiedUpload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset_id: str
    dataset_name: str
    document_id: str
    filename: str
    sha256: str
    provider_scope: str


class IngestionExecution(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    generation: int = Field(default=0, ge=0, strict=True)
    step: int = Field(default=0, ge=0, strict=True)
    phase: Literal["prepare", "poll"] = "prepare"
    upload: VerifiedUpload | None = None
    deadline: AwareDatetime | None = None
    interval: float = Field(default=1, ge=0.1, le=60, allow_inf_nan=False)
    ready_at: AwareDatetime | None = None
    last_run: str | None = None
    read_error: str | None = None

    @model_validator(mode="after")
    def valid_cursor(self) -> IngestionExecution:
        if self.phase == "poll" and (self.upload is None or self.deadline is None):
            raise ValueError("poll cursor is missing its upload or deadline")
        if self.deadline is not None and self.upload is None:
            raise ValueError("parse deadline requires a verified upload")
        if self.step > 0 and (self.ready_at is None or self.deadline is None):
            raise ValueError("scheduled cursor requires its original timer")
        return self


def execution_state(job: KnowledgeIngestionJob) -> IngestionExecution:
    first = (job.status_history or [{}])[0]
    if (first.get("metadata") or {}).get(EXECUTION_KEY) != EXECUTION_MARKER:
        raise ForbiddenError(
            "legacy ingestion execution requires explicit investigation"
        )
    return IngestionExecution.model_validate(
        (job.metadata_json or {}).get(EXECUTION_KEY)
    )


def save_execution(job: KnowledgeIngestionJob, state: IngestionExecution) -> None:
    state = IngestionExecution.model_validate(state.model_dump())
    metadata = {
        **(job.metadata_json or {}),
        EXECUTION_KEY: state.model_dump(mode="json"),
    }
    if job.metadata_json != metadata:
        job.metadata_json = metadata


async def database_now(session: AsyncSession) -> datetime:
    return (await session.execute(text("SELECT clock_timestamp()"))).scalar_one()


def guard_generation(
    session: AsyncSession, job: KnowledgeIngestionJob, generation: int
) -> None:
    """Fence both explicit commits and ORM autoflush against concurrent retries.

    Listeners live for this consumer session, including its final Inbox commit. They
    must not be removed on handler return, which occurs BEFORE the runtime commits.
    """
    job_id = job.id
    session.info["ingestion_fence"] = (job_id, generation)

    def check(sync_session, *unused):
        row = sync_session.execute(
            text(
                "SELECT metadata_json FROM knowledge_ingestion_job "
                "WHERE id=:id FOR UPDATE"
            ),
            {"id": job_id},
        ).scalar_one_or_none()
        if (
            row is None
            or (row.get(EXECUTION_KEY) or {}).get("generation") != generation
        ):
            raise DeliveryLeaseLost("ingestion retry generation changed")

    event.listen(session.sync_session, "before_flush", check)
    event.listen(session.sync_session, "before_commit", check)


async def assert_ingestion_current(session: AsyncSession) -> None:
    fence = session.info.get("ingestion_fence")
    if fence is None:
        return
    job_id, generation = fence
    metadata = (
        await session.execute(
            text("SELECT metadata_json FROM knowledge_ingestion_job WHERE id=:id"),
            {"id": job_id},
        )
    ).scalar_one_or_none()
    if (
        metadata is None
        or (metadata.get(EXECUTION_KEY) or {}).get("generation") != generation
    ):
        raise DeliveryLeaseLost("ingestion retry generation changed")
