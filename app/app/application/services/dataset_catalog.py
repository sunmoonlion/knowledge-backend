"""数据集目录（0008-info 段三）：默认数据集 + 登记表里的现行数据集。

没有打开多数据集时，目录里只有默认数据集，行为与以前完全一样。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from app.application.ports.datasets import DatasetFiles, DatasetRegistry
from app.application.services.dataset_query import DatasetInfo
from app.domain.datasets import RegisteredDataset

log = logging.getLogger(__name__)


class UnknownDataset(LookupError):
    """没有这个数据集。消息可以直接给模型看。"""


class DatasetQueries(Protocol):
    """一个数据集能回答的问题。只读 SQLite 与语义层两种实现都满足它。"""

    dataset_id: str

    def info(self) -> DatasetInfo: ...

    def describe_schema(self, table: str | None = None) -> dict[str, Any]: ...

    def metric_definitions(self, metric: str | None = None) -> dict[str, Any]: ...

    def run_sql(self, sql: str, *, max_rows: int | None = None) -> dict[str, Any]: ...


class DatasetCatalog:
    def __init__(
        self,
        *,
        default: DatasetQueries,
        ensure_default: Callable[[], None],
        default_title: str,
        registry: DatasetRegistry | None = None,
        files: DatasetFiles | None = None,
        open_dataset: Callable[[Path, str], DatasetQueries] | None = None,
        ttl_seconds: float = 30.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if (registry is None) != (files is None):
            raise ValueError("registry and files must be configured together")
        if registry is not None and open_dataset is None:
            raise ValueError("open_dataset is required when a registry is configured")
        self._default = default
        self._ensure_default = ensure_default
        self._default_title = default_title
        self._registry = registry
        self._files = files
        self._open = open_dataset
        self._ttl = ttl_seconds
        self._now = monotonic
        self._known: dict[str, RegisteredDataset] = {}
        self._loaded_at: float | None = None
        self._services: dict[tuple[str, str], DatasetQueries] = {}
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

    async def resolve(self, dataset_id: str | None) -> DatasetQueries:
        """按标识取查询服务。不给标识就是默认数据集。"""
        if dataset_id is None or dataset_id == self.default_id:
            await asyncio.to_thread(self._ensure_default)
            return self._default
        if not isinstance(dataset_id, str) or not dataset_id:
            raise UnknownDataset("dataset must be a non-empty string")
        await self._refresh()
        entry = self._known.get(dataset_id)
        if entry is None or self._files is None or self._open is None:
            raise UnknownDataset(
                f"unknown dataset: {dataset_id}; call list_datasets to see what exists"
            )
        key = (entry.dataset_id, entry.data_version)
        service = self._services.get(key)
        if service is None:
            path: Path = await asyncio.to_thread(self._files.ensure, entry)
            service = self._open(path, entry.dataset_id)
            self._services[key] = service
        return service

    async def describe(self) -> list[dict[str, object]]:
        """工具 `list_datasets` 给模型看的那一份。"""
        return [
            {k: v for k, v in entry.items() if k != "updated_at"}
            for entry in await self.listing()
        ]

    async def listing(self) -> list[dict[str, object]]:
        """现有的数据集。页面与工具列的是这同一份（F-KNOW-11），页面多一个更新时间。"""
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
                "updated_at": None,
            }
        ]
        for entry in sorted(self._known.values(), key=lambda r: r.dataset_id):
            listed.append(
                {
                    **entry.summary(),
                    "default": False,
                    "updated_at": entry.registered_at,
                }
            )
        return listed
