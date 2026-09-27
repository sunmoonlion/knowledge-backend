"""DuckDB 这一侧：转换、只读执行、超时中断、类型规整（0009-semantic）。"""

from __future__ import annotations

import datetime as dt
import math
import os
import re
import threading
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from app.application.ports.semantic import QueryResult
from app.application.services.dataset_query import SqlRejected
from app.domain.semantic import (
    ATTACHED_AS,
    DECIMAL,
    INTEGER,
    PHYSICAL_PREFIX,
    PHYSICAL_SCHEMA,
    DatasetDescription,
    SemanticModelError,
    physical_name,
)
from app.infrastructure.semantic.sqlite_source import connect

_BATCH = 50_000
_ARROW = {INTEGER: pa.int64(), DECIMAL: pa.float64()}
_MEMORY_LIMIT = "512MB"
_THREADS = 2
_PHYSICAL = re.compile(
    rf'"?\b(?:{ATTACHED_AS}\.)?(?:{PHYSICAL_SCHEMA}\.)?{PHYSICAL_PREFIX}(\w+)"?', re.I
)
_PATH = re.compile(r"(?:/[\w.\-]+){2,}")
_EXCERPT = re.compile(
    r"\s*(?:Candidate bindings|Candidate tables|Did you mean|LINE \d+:)"
)
_MAX_MESSAGE = 300


def convert(source: Path, description: DatasetDescription, target: Path) -> Path:
    """把 SQLite 数据集原样写进一个 DuckDB 文件；物理表加前缀，类型照搬。

    先写临时文件再改名：目标文件要么不存在，要么是完整的。
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(f"{target.name}.{uuid.uuid4().hex}.partial")
    try:
        with connect(source) as src:
            db = duckdb.connect(str(partial))
            try:
                for table in sorted(description.tables, key=lambda t: t.name):
                    _copy_table(src, db, table)
            finally:
                db.close()
        os.replace(partial, target)
    finally:
        for leftover in (partial, partial.with_name(partial.name + ".wal")):
            if leftover.exists():
                leftover.unlink()
    return target


def _copy_table(src, db, table) -> None:
    physical = physical_name(table.name)
    ddl = ", ".join(f'"{c.name}" {c.type}' for c in table.columns)
    db.execute(f'CREATE TABLE "{physical}" ({ddl})')
    names = ", ".join(f'"{c.name}"' for c in table.columns)
    cursor = src.execute(f'SELECT {names} FROM "{table.name}"')  # noqa: S608
    copied = 0
    while True:
        rows = cursor.fetchmany(_BATCH)
        if not rows:
            break
        try:
            batch = pa.table(
                {
                    c.name: pa.array(
                        [row[i] for row in rows], type=_ARROW.get(c.type, pa.string())
                    )
                    for i, c in enumerate(table.columns)
                }
            )
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # SQLite 不强制类型；值与声明的类型对不上时不猜
            raise SemanticModelError(
                f"values do not match the declared types: {table.name}"
            ) from None
        db.register("incoming", batch)
        db.execute(f'INSERT INTO "{physical}" SELECT * FROM incoming')  # noqa: S608
        db.unregister("incoming")
        copied += len(rows)
    if copied != table.row_count:
        raise SemanticModelError(f"row count changed while converting: {table.name}")


def sanitize(message: str) -> str:
    """给模型看的报错：只留说明，物理表名换回模型名，去掉路径，截短（F-SEM-05）。

    库的报错后面跟着候选字段与出错位置的 SQL 片段；那是规划之后的 SQL，
    不是用户写的那一条，给出去只会误导，还会带出内部的名字。
    """
    text = _EXCERPT.split(message, maxsplit=1)[0]
    text = _PHYSICAL.sub(lambda m: m.group(1), text)
    text = _PATH.sub("<path>", text)
    text = " ".join(text.split())
    return text[:_MAX_MESSAGE]


def normalize(value: Any) -> Any:
    """返回值只有：整数、小数、文字、布尔、空；其余转成文字（F-SEM-06）。"""
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dt.datetime | dt.date | dt.time):
        return value.isoformat()
    if isinstance(value, bytes | bytearray | memoryview):
        raise SqlRejected("binary values cannot be returned")
    if isinstance(value, list | tuple):
        return [normalize(v) for v in value]
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    return str(value)


class DuckDbExecutor:
    """一个数据集一个实例。库只读挂载，对外访问关闭，配置锁定。"""

    def __init__(self, database: Path) -> None:
        if not database.is_file():
            raise FileNotFoundError(f"semantic database missing: {database}")
        self._root = duckdb.connect(":memory:")
        escaped = str(database.resolve()).replace("'", "''")
        self._root.execute(
            f"ATTACH DATABASE '{escaped}' AS \"{ATTACHED_AS}\" (READ_ONLY)"
        )
        self._root.execute(f"SET memory_limit='{_MEMORY_LIMIT}'")
        self._root.execute(f"SET threads={_THREADS}")
        self._root.execute("SET autoinstall_known_extensions=false")
        self._root.execute("SET autoload_known_extensions=false")
        self._root.execute("SET enable_external_access=false")
        self._root.execute("SET lock_configuration=true")

    def close(self) -> None:
        self._root.close()

    def execute(self, planned_sql: str, *, limit: int, timeout_ms: int) -> QueryResult:
        cursor = self._root.cursor()
        expired = threading.Event()

        def interrupt() -> None:
            expired.set()
            cursor.interrupt()

        timer = threading.Timer(timeout_ms / 1000, interrupt)
        timer.daemon = True
        timer.start()
        try:
            cursor.execute(planned_sql)
            columns = [str(d[0]) for d in cursor.description or ()]
            fetched = cursor.fetchmany(limit + 1)
        except duckdb.InterruptException:
            raise SqlRejected(
                f"query exceeded {timeout_ms} ms and was cancelled"
            ) from None
        except duckdb.Error as exc:
            if expired.is_set():
                raise SqlRejected(
                    f"query exceeded {timeout_ms} ms and was cancelled"
                ) from None
            raise SqlRejected(f"query failed: {sanitize(str(exc))}") from None
        finally:
            timer.cancel()
            cursor.close()
        if len(set(columns)) != len(columns):
            raise SqlRejected(
                "the result has duplicate column names; give each column an alias"
            )
        rows = [
            {name: normalize(value) for name, value in zip(columns, row, strict=True)}
            for row in fetched[:limit]
        ]
        return QueryResult(columns, rows, truncated=len(fetched) > limit)
