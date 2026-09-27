"""经语义层的数据集查询（0009-semantic）。方法与只读 SQLite 的查询服务一一对应。

一条查询的路：前置检查 → 引擎按语义模型规划 → 我们自己只读执行 → 类型规整 → 带上出处。
"""

from __future__ import annotations

import hashlib
import time
from datetime import UTC, datetime
from typing import Any

from app.application.ports.semantic import SqlExecutor, SqlPlanner
from app.application.services import semantic_guard, semantic_metrics
from app.application.services.dataset_query import (
    MAX_ROWS,
    DatasetInfo,
    DatasetQueryService,
    SqlRejected,
)
from app.domain.semantic import DatasetDescription


class SemanticQueryService:
    def __init__(
        self,
        *,
        dataset_id: str,
        description: DatasetDescription,
        planner: SqlPlanner,
        executor: SqlExecutor,
        engine: str,
        fallback_version: str,
        max_rows: int = MAX_ROWS,
        timeout_ms: int = 5000,
    ) -> None:
        self.dataset_id = dataset_id
        self.max_rows = max_rows
        self.timeout_ms = timeout_ms
        self._description = description
        self._planner = planner
        self._executor = executor
        self._engine = engine
        self._fallback_version = fallback_version
        self._metrics = semantic_metrics.queryable_metrics(description)

    def info(self) -> DatasetInfo:
        meta = self._description.metadata
        version = meta.get("data_snapshot_id") or self._fallback_version
        return DatasetInfo(
            dataset_id=self.dataset_id,
            data_version=version,
            source=f"{self.dataset_id}@{version}",
            start_date=meta.get("start_date"),
            end_date=meta.get("end_date"),
        )

    def _citation(self, **extra: Any) -> dict[str, Any]:
        info = self.info()
        return {
            "dataset_id": info.dataset_id,
            "data_version": info.data_version,
            "source": info.source,
            "as_of": info.end_date,
            "retrieved_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "engine": self._engine,
            **extra,
        }

    def describe_schema(self, table: str | None = None) -> dict[str, Any]:
        tables = sorted(self._description.tables, key=lambda t: t.name)
        if table is not None:
            tables = [t for t in tables if t.name == table]
            if not tables:
                raise SqlRejected(f"unknown table: {table}")
        return {
            "tables": [
                {
                    "name": t.name,
                    "columns": [
                        {
                            "name": c.name,
                            "type": c.type,
                            "display_name": c.display_name,
                            "unit": c.unit,
                        }
                        for c in t.columns
                    ],
                    "row_count": t.row_count,
                }
                for t in tables
            ],
            "sql_dialect": "duckdb",
            "citation": self._citation(),
        }

    def metric_definitions(self, metric: str | None = None) -> dict[str, Any]:
        if self._description.table("metric_dictionary") is None:
            raise SqlRejected("this dataset has no metric_dictionary")
        rows = [dict(r) for r in self._description.metrics]
        rows.sort(key=lambda r: str(r.get("metric_name")))
        if metric is not None:
            rows = DatasetQueryService._match_metrics(rows, metric)
        for row in rows:
            # 以校验过的为准：数据集说可以查、但定义不合规的，这里是不可查
            row["queryable"] = str(row.get("metric_name")) in self._metrics
        return {"metrics": rows, "citation": self._citation()}

    def query_metric(
        self,
        metrics: Any,
        *,
        filters: Any = None,
        order_by: Any = None,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        """按口径名查询：值由口径的定义算出，是否适用由定义里的条件判定。"""
        limit = self._limit(max_rows)
        query = semantic_metrics.build_query(
            self._description,
            self._metrics,
            metrics=metrics,
            filters=filters,
            order_by=order_by,
        )
        started = time.monotonic()
        body = semantic_guard.check(query.sql)
        planned = self._planner.plan(body)
        result = self._executor.execute(
            planned, limit=limit, timeout_ms=self.timeout_ms
        )
        return {
            "definitions": [
                {
                    "metric_name": m.name,
                    "display_name": m.display_name,
                    "unit": m.unit,
                    "base_table": m.base_table,
                    "formula": m.formula,
                    "applicable_when": m.applicable_when,
                    "description": m.description,
                }
                for m in query.metrics
            ],
            "rows": semantic_metrics.shape(result.rows, query),
            "row_count": len(result.rows),
            "truncated": result.truncated,
            "sql": body,
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "citation": self._citation(
                query_digest=hashlib.sha256(body.encode()).hexdigest()[:16]
            ),
        }

    def run_sql(self, sql: str, *, max_rows: int | None = None) -> dict[str, Any]:
        body = semantic_guard.check(sql)
        limit = self._limit(max_rows)
        started = time.monotonic()
        planned = self._planner.plan(body)
        result = self._executor.execute(
            planned, limit=limit, timeout_ms=self.timeout_ms
        )
        return {
            "columns": result.columns,
            "rows": result.rows,
            "row_count": len(result.rows),
            "truncated": result.truncated,
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "citation": self._citation(
                query_digest=hashlib.sha256(body.encode()).hexdigest()[:16]
            ),
        }

    def _limit(self, max_rows: Any) -> int:
        if max_rows is None:
            return self.max_rows
        if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows < 1:
            raise SqlRejected("max_rows must be a positive integer")
        return min(max_rows, self.max_rows)
