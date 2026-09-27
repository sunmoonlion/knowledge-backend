"""一个数据集在语义层上的全部：自述、转换、语义模型、规划器、执行器（0009-semantic）。

第一次用到时才准备。转换出的库按「数据集文件的校验值 + 生成规则版本」放在缓存目录，
已有的直接用；用之前核对每张表的行数，对不上就重建。
"""

from __future__ import annotations

import hashlib
import logging
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb

from app.application.services.dataset_query import DatasetInfo
from app.application.services.semantic_model import build_manifest
from app.application.services.semantic_query import SemanticQueryService
from app.domain.semantic import (
    ATTACHED_AS,
    BUILDER_VERSION,
    DatasetDescription,
    SemanticModelError,
    physical_name,
)
from app.infrastructure.external.dataset_store import DatasetUnavailable
from app.infrastructure.semantic.duckdb_store import DuckDbExecutor, convert
from app.infrastructure.semantic.sqlite_source import read_description
from app.infrastructure.semantic.wren_planner import ENGINE, WrenPlanner

log = logging.getLogger(__name__)


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _intact(database: Path, description: DatasetDescription) -> bool:
    try:
        db = duckdb.connect(":memory:")
        try:
            escaped = str(database.resolve()).replace("'", "''")
            db.execute(f"ATTACH DATABASE '{escaped}' AS \"{ATTACHED_AS}\" (READ_ONLY)")
            for table in description.tables:
                name = f'"{ATTACHED_AS}"."main"."{physical_name(table.name)}"'
                count = db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()  # noqa: S608
                if count is None or int(count[0]) != table.row_count:
                    return False
        finally:
            db.close()
    except duckdb.Error:
        return False
    return True


class SemanticDataset:
    def __init__(
        self,
        path: Path,
        *,
        dataset_id: str,
        cache_dir: Path,
        ensure: Callable[[], None] | None = None,
        max_rows: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        self.path = Path(path)
        self.dataset_id = dataset_id
        self._cache_dir = Path(cache_dir)
        self._ensure = ensure
        self._options: dict[str, int] = {}
        if max_rows is not None:
            self._options["max_rows"] = max_rows
        if timeout_ms is not None:
            self._options["timeout_ms"] = timeout_ms
        self._service: SemanticQueryService | None = None
        self._executor: DuckDbExecutor | None = None
        self._lock = threading.Lock()

    def _prepared(self) -> SemanticQueryService:
        if self._service is not None:
            return self._service
        with self._lock:
            if self._service is not None:
                return self._service
            if self._ensure is not None:
                self._ensure()
            try:
                description = read_description(self.path)
                digest = file_digest(self.path)
                database = self._cache_dir / f"{digest}-b{BUILDER_VERSION}.duckdb"
                if not (database.is_file() and _intact(database, description)):
                    convert(self.path, description, database)
                manifest = build_manifest(description)
                executor = DuckDbExecutor(database)
                service = SemanticQueryService(
                    dataset_id=self.dataset_id,
                    description=description,
                    planner=WrenPlanner(manifest),
                    executor=executor,
                    engine=ENGINE,
                    fallback_version=digest[:16],
                    **self._options,
                )
            except SemanticModelError as exc:
                log.error(
                    "semantic_dataset_rejected dataset=%s reason=%s",
                    self.dataset_id,
                    exc,
                )
                raise DatasetUnavailable(
                    "dataset cannot be served by the semantic layer"
                ) from None
            self._executor = executor
            self._service = service
            return service

    def close(self) -> None:
        with self._lock:
            if self._executor is not None:
                self._executor.close()
            self._executor = None
            self._service = None

    def info(self) -> DatasetInfo:
        return self._prepared().info()

    def describe_schema(self, table: str | None = None) -> dict[str, Any]:
        return self._prepared().describe_schema(table)

    def metric_definitions(self, metric: str | None = None) -> dict[str, Any]:
        return self._prepared().metric_definitions(metric)

    def run_sql(self, sql: str, *, max_rows: int | None = None) -> dict[str, Any]:
        return self._prepared().run_sql(sql, max_rows=max_rows)

    def query_metric(
        self,
        metrics: Any,
        *,
        filters: Any = None,
        order_by: Any = None,
        max_rows: int | None = None,
    ) -> dict[str, Any]:
        return self._prepared().query_metric(
            metrics, filters=filters, order_by=order_by, max_rows=max_rows
        )
