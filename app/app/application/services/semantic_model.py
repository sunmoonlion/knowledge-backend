"""从数据集的自述生成语义模型（0009-semantic，F-SEM-02）。纯函数，同样的输入得到同样的输出。"""

from __future__ import annotations

from typing import Any

from app.domain.semantic import (
    ATTACHED_AS,
    PHYSICAL_SCHEMA,
    DatasetDescription,
    TableSpec,
    physical_name,
)

LAYOUT_VERSION = 3
CATALOG = "wren"
SCHEMA = "public"


def _column(table: TableSpec, index: int) -> dict[str, Any]:
    column = table.columns[index]
    properties: dict[str, str] = {}
    if column.display_name:
        properties["displayName"] = column.display_name
        properties["description"] = (
            f"{column.display_name}（单位 {column.unit}）"
            if column.unit
            else column.display_name
        )
    return {
        "name": column.name,
        "type": column.type,
        "isCalculated": False,
        "notNull": False,
        "properties": properties,
    }


def build_manifest(description: DatasetDescription) -> dict[str, Any]:
    """每张表一个模型：模型名是原表名，物理表是加了前缀的那一张。"""
    description.validate()
    models = [
        {
            "name": table.name,
            "tableReference": {
                "catalog": ATTACHED_AS,
                "schema": PHYSICAL_SCHEMA,
                "table": physical_name(table.name),
            },
            "columns": [_column(table, i) for i in range(len(table.columns))],
            "cached": False,
            "properties": {},
        }
        for table in sorted(description.tables, key=lambda t: t.name)
    ]
    return {
        "catalog": CATALOG,
        "schema": SCHEMA,
        "models": models,
        "relationships": [],
        "views": [],
        "cubes": [],
        "dataSource": "duckdb",
        "layoutVersion": LAYOUT_VERSION,
    }
