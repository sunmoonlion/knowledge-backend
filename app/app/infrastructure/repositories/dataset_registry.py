"""数据集登记表（PostgreSQL）。"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.datasets import (
    ACTIVE,
    SUPERSEDED,
    DatasetRegistration,
    DatasetVersionConflict,
    RegisteredDataset,
)
from app.infrastructure.models.datasets import KnowledgeDataset


def _entity(row: KnowledgeDataset) -> RegisteredDataset:
    return RegisteredDataset(
        dataset_id=row.dataset_id,
        data_version=row.data_version,
        title=row.title,
        bucket=row.bucket,
        object_key=row.object_key,
        object_version_id=row.object_version_id,
        sha256=row.sha256,
        size_bytes=row.size_bytes,
        start_date=row.start_date,
        end_date=row.end_date,
        security_code=row.security_code,
        source_app=row.source_app,
        source_ref=row.source_ref,
        status=row.status,
        registered_by=row.registered_by,
        registered_at=row.registered_at,
    )


class SqlDatasetRegistry:
    def __init__(
        self,
        sessions: Callable[[], async_sessionmaker[AsyncSession]],
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sessions = sessions  # 取会话工厂的函数：数据库在应用启动后才初始化
        self._clock = clock

    async def register(
        self, registration: DatasetRegistration, *, registered_by: str
    ) -> RegisteredDataset:
        async with self._sessions()() as session:
            # 同一数据集的登记串行化。行锁不够：第一次登记时还没有行可锁，
            # 所以用按数据集标识取的事务级咨询锁，事务结束自动释放。
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"knowledge_dataset:{registration.dataset_id}"},
            )
            rows = (
                (
                    await session.execute(
                        select(KnowledgeDataset).where(
                            KnowledgeDataset.dataset_id == registration.dataset_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            same = next(
                (r for r in rows if r.data_version == registration.data_version), None
            )
            if same is not None:
                if (
                    same.sha256 != registration.sha256
                    or same.bucket != registration.bucket
                    or same.object_key != registration.object_key
                ):
                    raise DatasetVersionConflict(
                        "this data_version is already registered with different content"
                    )
                if same.status == ACTIVE:
                    return _entity(same)
            await session.execute(
                update(KnowledgeDataset)
                .where(
                    KnowledgeDataset.dataset_id == registration.dataset_id,
                    KnowledgeDataset.status == ACTIVE,
                )
                .values(status=SUPERSEDED)
            )
            await session.flush()
            if same is not None:  # 重新启用一个曾被取代的版本
                same.status = ACTIVE
                row = same
            else:
                row = KnowledgeDataset(
                    dataset_id=registration.dataset_id,
                    data_version=registration.data_version,
                    title=registration.title.strip(),
                    security_code=registration.security_code,
                    bucket=registration.bucket,
                    object_key=registration.object_key,
                    object_version_id=registration.object_version_id,
                    sha256=registration.sha256,
                    size_bytes=registration.size_bytes,
                    start_date=registration.start_date,
                    end_date=registration.end_date,
                    source_app=registration.source_app,
                    source_ref=registration.source_ref,
                    status=ACTIVE,
                    registered_by=registered_by,
                    registered_at=self._clock(),
                )
                session.add(row)
            await session.flush()
            entity = _entity(row)
            await session.commit()
            return entity

    async def active(self) -> list[RegisteredDataset]:
        async with self._sessions()() as session:
            rows = await session.execute(
                select(KnowledgeDataset)
                .where(KnowledgeDataset.status == ACTIVE)
                .order_by(KnowledgeDataset.dataset_id)
            )
            return [_entity(r) for r in rows.scalars()]

    async def get_active(self, dataset_id: str) -> RegisteredDataset | None:
        async with self._sessions()() as session:
            row = (
                await session.execute(
                    select(KnowledgeDataset).where(
                        KnowledgeDataset.dataset_id == dataset_id,
                        KnowledgeDataset.status == ACTIVE,
                    )
                )
            ).scalar_one_or_none()
            return _entity(row) if row else None
