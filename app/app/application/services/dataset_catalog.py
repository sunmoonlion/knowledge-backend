"""数据集目录（0008-info 段三）：默认数据集 + 登记表里的现行数据集。

没有打开多数据集时，目录里只有默认数据集，行为与以前完全一样。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from pathlib import Path

from app.application.ports.datasets import DatasetFiles, DatasetRegistry
from app.application.services.dataset_query import DatasetQueryService
from app.domain.datasets import RegisteredDataset

log = logging.getLogger(__name__)


class UnknownDataset(LookupError):
    """没有这个数据集。消息可以直接给模型看。"""


class DatasetCatalog:
    def __init__(
        self,
        *,
        default: DatasetQueryService,
        ensure_default: Callable[[], None],
        default_title: str,
        registry: DatasetRegistry | None = None,
        files: DatasetFiles | None = None,
        ttl_seconds: float = 30.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if (registry is None) != (files is None):
            raise ValueError("registry and files must be configured together")
        self._default = default
        self._ensure_default = ensure_default
        self._default_title = default_title
        self._registry = registry
        self._files = files
        self._ttl = ttl_seconds
        self._now = monotonic
        self._known: dict[str, RegisteredDataset] = {}
        self._loaded_at: float | None = None
        self._services: dict[tuple[str, str], DatasetQueryService] = {}
        self._lock = asyncio.Lock()

    @property
    def default_id(self) -> str:
        return self._default.dataset_id

    async def _refresh(self) -> None:
        if self._registry is None:
            return
        fresh = (
            self._loaded_at is not None and self._now() - self._loaded_at < self._ttl
        )
        if fresh:
            return
        async with self._lock:
            rows = await self._registry.active()
            self._known = {
                r.dataset_id: r for r in rows if r.dataset_id != self.default_id
            }
            self._loaded_at = self._now()
            live = {(r.dataset_id, r.data_version) for r in rows}
            for key in [k for k in self._services if k not in live]:
                del self._services[key]  # 被新版本取代的旧版本不再提供查询

    async def resolve(self, dataset_id: str | None) -> DatasetQueryService:
        """按标识取查询服务。不给标识就是默认数据集。"""
        if dataset_id is None or dataset_id == self.default_id:
            await asyncio.to_thread(self._ensure_default)
            return self._default
        if not isinstance(dataset_id, str) or not dataset_id:
            raise UnknownDataset("dataset must be a non-empty string")
        await self._refresh()
        entry = self._known.get(dataset_id)
        if entry is None or self._files is None:
            raise UnknownDataset(
                f"unknown dataset: {dataset_id}; call list_datasets to see what exists"
            )
        key = (entry.dataset_id, entry.data_version)
        service = self._services.get(key)
        if service is None:
            path: Path = await asyncio.to_thread(self._files.ensure, entry)
            service = DatasetQueryService(path, dataset_id=entry.dataset_id)
            self._services[key] = service
        return service

    async def describe(self) -> list[dict[str, object]]:
        await self._refresh()
        await asyncio.to_thread(self._ensure_default)
        info = await asyncio.to_thread(self._default.info)
        listed: list[dict[str, object]] = [
            {
                "dataset": self.default_id,
                "title": self._default_title,
                "security_code": None,
                "data_version": info.data_version,
                "start_date": info.start_date,
                "end_date": info.end_date,
                "default": True,
            }
        ]
        for entry in sorted(self._known.values(), key=lambda r: r.dataset_id):
            listed.append({**entry.summary(), "default": False})
        return listed
