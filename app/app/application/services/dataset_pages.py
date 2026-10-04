"""数据目录页面背后的读法（PRD/apps/knowledge.md 第五节）。

列表用数据集目录，就是工具 `list_datasets` 列的那一份；表与列、口径、说明用的是
工具背后的同一套查询。只读结构：除了数据集自带的说明表，不读任何一行数据。
"""

from __future__ import annotations

import asyncio
from typing import Any

from app.application.services.dataset_catalog import (
    DatasetCatalog,
    DatasetQueries,
    UnknownDataset,
)
from app.application.services.dataset_query import SqlRejected
from app.domain.dataset_view import DICTIONARY_TABLES, detail_view, matches

# 一张表的列说明、一个数据集的说明，都远少于这个数；超了就是数据集不对，宁可少显示
_MAX_NOTES = 200


class DatasetPages:
    def __init__(self, catalog: DatasetCatalog, *, registry_enabled: bool) -> None:
        self._catalog = catalog
        self._registry_enabled = registry_enabled
        # 同一个版本的结构不会变：按（数据集，版本）记住，换了版本自然失效
        self._details: dict[tuple[str, str], dict[str, Any]] = {}

    async def listing(self, query: str | None = None) -> dict[str, Any]:
        entries = await self._catalog.listing()
        return {
            "registry_enabled": self._registry_enabled,
            "total": len(entries),
            "datasets": [e for e in entries if matches(e, query)],
        }

    async def detail(self, dataset_id: str) -> dict[str, Any]:
        entry = next(
            (e for e in await self._catalog.listing() if e["dataset"] == dataset_id),
            None,
        )
        if entry is None:
            raise UnknownDataset(f"unknown dataset: {dataset_id}")
        key = (dataset_id, str(entry["data_version"]))
        if key not in self._details:
            queries = await self._catalog.resolve(dataset_id)
            structure = await asyncio.to_thread(_structure, queries)
            for stale in [k for k in self._details if k[0] == dataset_id]:
                del self._details[stale]
            self._details[key] = structure
        return detail_view(entry, **self._details[key])


def _rows(queries: DatasetQueries, sql: str) -> list[dict[str, Any]]:
    """读一张说明表。数据集没有这张表就当作没有说明。"""
    try:
        return list(queries.run_sql(sql, max_rows=_MAX_NOTES)["rows"])
    except SqlRejected:
        return []


def _structure(queries: DatasetQueries) -> dict[str, Any]:
    tables = list(queries.describe_schema()["tables"])
    try:
        metrics = list(queries.metric_definitions()["metrics"])
    except SqlRejected:
        metrics = []
    metadata = {
        str(r["key"]): str(r["value"])
        for r in _rows(queries, "SELECT key, value FROM dataset_metadata")
        if r.get("key") is not None and r.get("value") is not None
    }
    labels: dict[str, dict[str, dict[str, Any]]] = {}
    for table in tables:
        name = str(table["name"])
        if name in DICTIONARY_TABLES:
            continue
        quoted = name.replace("'", "''")
        labels[name] = {
            str(r["field"]): {
                "display_name": r.get("display_name"),
                "unit": r.get("unit"),
            }
            for r in _rows(
                queries,
                "SELECT field, display_name, unit FROM field_dictionary "
                f"WHERE source_table = '{quoted}'",  # noqa: S608 表名来自数据集自己的表清单
            )
        }
    return {
        "tables": tables,
        "metrics": metrics,
        "metadata": metadata,
        "labels": labels,
    }
