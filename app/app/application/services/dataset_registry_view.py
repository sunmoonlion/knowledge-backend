"""管理端看登记表（F-KNOW-12）：每个数据集的现行版本，和它的版本历史。只读。"""

from __future__ import annotations

from typing import Any

from app.application.ports.datasets import DatasetFileState, DatasetHistory
from app.domain.datasets import ACTIVE, RegisteredDataset


class DatasetRegistryView:
    def __init__(self, history: DatasetHistory, files: DatasetFileState) -> None:
        self._history = history
        self._files = files

    def _version(self, row: RegisteredDataset) -> dict[str, Any]:
        return {
            "dataset_id": row.dataset_id,
            "data_version": row.data_version,
            "title": row.title,
            "security_code": row.security_code,
            "status": row.status,
            "start_date": row.start_date,
            "end_date": row.end_date,
            "sha256": row.sha256,
            "size_bytes": row.size_bytes,
            "bucket": row.bucket,
            "object_key": row.object_key,
            "object_version_id": row.object_version_id,
            "source_app": row.source_app,
            "source_ref": row.source_ref,
            "registered_by": row.registered_by,
            "registered_at": row.registered_at,
            "superseded_at": None if row.status == ACTIVE else row.changed_at,
            "fetched": self._files.fetched(row),
        }

    async def datasets(self) -> list[dict[str, Any]]:
        grouped: dict[str, list[RegisteredDataset]] = {}
        for row in await self._history.versions():
            grouped.setdefault(row.dataset_id, []).append(row)
        listed = []
        for dataset_id, rows in sorted(grouped.items()):
            current = next((r for r in rows if r.status == ACTIVE), None)
            shown = current or rows[0]
            listed.append(
                {
                    "dataset_id": dataset_id,
                    "title": shown.title,
                    "security_code": shown.security_code,
                    "current": self._version(current) if current else None,
                    "version_count": len(rows),
                }
            )
        return listed

    async def versions(self, dataset_id: str) -> list[dict[str, Any]]:
        """现行的排最前，其余按登记时间从新到旧。"""
        rows = await self._history.versions(dataset_id)
        rows.sort(key=lambda r: r.status != ACTIVE)
        return [self._version(r) for r in rows]
