"""读 SQLite 数据集的自述：表、字段、字段字典、口径表、元数据（0009-semantic）。"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from app.domain.semantic import (
    DECIMAL,
    INTEGER,
    TEXT,
    ColumnSpec,
    DatasetDescription,
    SemanticModelError,
    TableSpec,
)


def connect(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(f"dataset file missing: {path}")
    conn = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only = ON")
    return conn


def column_type(table: str, column: str, declared: str) -> str:
    """SQLite 声明类型到引擎类型。认不出的不猜：宁可这份数据集上不了语义层。"""
    kind = declared.strip().upper()
    if "INT" in kind:
        return INTEGER
    if any(word in kind for word in ("CHAR", "CLOB", "TEXT")):
        return TEXT
    if any(word in kind for word in ("REAL", "FLOA", "DOUB")):
        return DECIMAL
    raise SemanticModelError(f"unsupported column type: {table}.{column}")


def table_names(conn: sqlite3.Connection) -> list[str]:
    return [
        str(r[0])
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]


def _rows(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    cursor = conn.execute(f'SELECT * FROM "{table}"')  # noqa: S608 表名来自 sqlite_master
    names = [str(d[0]) for d in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def read_description(path: Path) -> DatasetDescription:
    with connect(path) as conn:
        names = table_names(conn)
        dictionary: dict[tuple[str, str], tuple[str | None, str | None]] = {}
        if "field_dictionary" in names:
            for row in _rows(conn, "field_dictionary"):
                key = (str(row.get("source_table")), str(row.get("field")))
                dictionary[key] = (
                    str(row["display_name"]) if row.get("display_name") else None,
                    str(row["unit"]) if row.get("unit") else None,
                )
        tables = []
        for name in names:
            columns = []
            for info in conn.execute(f'PRAGMA table_info("{name}")'):
                column = str(info[1])
                display, unit = dictionary.get((name, column), (None, None))
                columns.append(
                    ColumnSpec(
                        name=column,
                        type=column_type(name, column, str(info[2] or "")),
                        display_name=display,
                        unit=unit,
                    )
                )
            count = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]  # noqa: S608
            tables.append(TableSpec(name, tuple(columns), int(count)))
        metrics = (
            tuple(_rows(conn, "metric_dictionary"))
            if "metric_dictionary" in names
            else ()
        )
        metadata = (
            {str(r["key"]): str(r["value"]) for r in _rows(conn, "dataset_metadata")}
            if "dataset_metadata" in names
            else {}
        )
    description = DatasetDescription(tuple(tables), metrics, metadata)
    description.validate()
    return description
