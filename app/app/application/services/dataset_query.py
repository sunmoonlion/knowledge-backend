"""数据集查询（0006「最小实例」）：一个只读 SQLite 数据集，给 MCP 三个工具用。

- 只读：`mode=ro` + `query_only`，单条 SELECT/WITH，禁 ATTACH/PRAGMA；
  超时中断；行数封顶（F-KNOW-05 不整批下发）。
- 每个结果带来源、时点、数据版本（F-KNOW-02 进证据账）；
  口径表随数据版本（F-KNOW-07）。
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MAX_ROWS = 200
_FORBIDDEN = re.compile(r"\b(attach|detach|pragma|vacuum|load_extension)\b", re.I)
_LEADING = re.compile(r"^\s*(with|select)\b", re.I)


class SqlRejected(ValueError):
    """SQL 不合只读规则；消息可直接给模型看。"""


@dataclass(frozen=True)
class DatasetInfo:
    dataset_id: str
    data_version: str
    source: str
    start_date: str | None
    end_date: str | None


class DatasetQueryService:
    def __init__(
        self,
        path: Path,
        *,
        dataset_id: str,
        max_rows: int = MAX_ROWS,
        timeout_ms: int = 5000,
    ) -> None:
        self.path = Path(path)
        self.dataset_id = dataset_id
        self.max_rows = max_rows
        self.timeout_ms = timeout_ms

    # ---------------- 连接 ----------------
    def _connect(self) -> sqlite3.Connection:
        if not self.path.is_file():
            raise FileNotFoundError(f"dataset file missing: {self.path}")
        conn = sqlite3.connect(f"file:{self.path.resolve()}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        return conn

    def info(self) -> DatasetInfo:
        with self._connect() as conn:
            meta: dict[str, str] = {}
            try:
                for k, v in conn.execute("SELECT key, value FROM dataset_metadata"):
                    meta[str(k)] = str(v)
            except sqlite3.Error:
                pass
        version = meta.get("data_snapshot_id") or self._file_digest()[:16]
        return DatasetInfo(
            dataset_id=self.dataset_id,
            data_version=version,
            source=f"{self.dataset_id}@{version}",
            start_date=meta.get("start_date"),
            end_date=meta.get("end_date"),
        )

    def _file_digest(self) -> str:
        h = hashlib.sha256()
        with self.path.open("rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()

    def _citation(self, info: DatasetInfo, **extra: Any) -> dict[str, Any]:
        return {
            "dataset_id": info.dataset_id,
            "data_version": info.data_version,
            "source": info.source,
            "as_of": info.end_date,
            "retrieved_at": datetime.now(UTC).isoformat(timespec="seconds"),
            **extra,
        }

    # ---------------- 工具 ----------------
    def describe_schema(self, table: str | None = None) -> dict[str, Any]:
        info = self.info()
        with self._connect() as conn:
            names = [
                str(r[0])
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
            ]
            if table is not None:
                if table not in names:
                    raise SqlRejected(f"unknown table: {table}")
                names = [table]
            tables = []
            for name in names:
                cols = [
                    {
                        "name": str(c[1]),
                        "type": str(c[2] or ""),
                        "primary_key": bool(c[5]),
                    }
                    for c in conn.execute(f'PRAGMA table_info("{name}")')
                ]
                count = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                tables.append({"name": name, "columns": cols, "row_count": int(count)})
        return {"tables": tables, "citation": self._citation(info)}

    def metric_definitions(self, metric: str | None = None) -> dict[str, Any]:
        info = self.info()
        with self._connect() as conn:
            try:
                cur = conn.execute("SELECT * FROM metric_dictionary ORDER BY 1")
            except sqlite3.Error as exc:
                raise SqlRejected("this dataset has no metric_dictionary") from exc
            rows = [dict(r) for r in cur.fetchall()]
        if metric is not None:
            rows = self._match_metrics(rows, metric)
        return {"metrics": rows, "citation": self._citation(info)}

    @staticmethod
    def _match_metrics(rows: list[dict[str, Any]], metric: str) -> list[dict[str, Any]]:
        """先按英文名或中文显示名精确匹配；都没有再按包含关系（不分大小写）找。

        专家常用中文名问（如"净营收"），以前只认英文名、返回空，
        只能把整本字典翻一遍（KIND 08 实测）。
        """
        key = metric.strip()
        fields = ("metric_name", "display_name")
        exact = [r for r in rows if any(str(r.get(f) or "") == key for f in fields)]
        if exact:
            return exact
        low = key.lower()
        return [
            r
            for r in rows
            if low and any(low in str(r.get(f) or "").lower() for f in fields)
        ]

    @classmethod
    def guard(cls, sql: str) -> str:
        body = sql.strip()
        if body.endswith(";"):
            body = body[:-1].rstrip()
        if not body:
            raise SqlRejected("empty SQL")
        if ";" in body:
            raise SqlRejected("exactly one statement is allowed")
        if not _LEADING.match(body):
            raise SqlRejected("only SELECT or WITH ... SELECT is allowed")
        if _FORBIDDEN.search(body):
            raise SqlRejected("ATTACH/PRAGMA/VACUUM/load_extension are not allowed")
        return body

    def run_sql(self, sql: str, *, max_rows: int | None = None) -> dict[str, Any]:
        body = self.guard(sql)
        limit = min(int(max_rows or self.max_rows), self.max_rows)
        info = self.info()
        started = time.monotonic()
        deadline = started + self.timeout_ms / 1000
        with self._connect() as conn:
            conn.set_progress_handler(
                lambda: 1 if time.monotonic() > deadline else 0, 1000
            )
            try:
                cur = conn.execute(body)
                columns = [str(d[0]) for d in cur.description or ()]
                fetched = cur.fetchmany(limit + 1)
            except sqlite3.OperationalError as exc:
                if "interrupted" in str(exc).lower():
                    raise SqlRejected(
                        f"query exceeded {self.timeout_ms} ms and was cancelled"
                    ) from exc
                raise SqlRejected(f"sqlite error: {exc}") from exc
            finally:
                conn.set_progress_handler(None, 0)
        truncated = len(fetched) > limit
        rows = [dict(r) for r in fetched[:limit]]
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "citation": self._citation(
                info,
                query_digest=hashlib.sha256(body.encode()).hexdigest()[:16],
            ),
        }
