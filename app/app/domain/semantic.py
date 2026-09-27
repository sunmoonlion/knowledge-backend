"""语义层的领域对象（0009-semantic）：数据集的自述与命名规则。不依赖框架、数据库与引擎。"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# 物理表统一加前缀，模型名用原表名：两者同名时，带库名的引用能绕过字段暴露（探针问题一）
PHYSICAL_PREFIX = "phys_"
# 口径在引擎里的名字加前缀：口径名与它聚合的字段同名会被判为循环依赖（探针问题二）
MEASURE_PREFIX = "m_"
ATTACHED_AS = "ds"
PHYSICAL_SCHEMA = "main"
# 生成规则变了就加一：缓存里按旧规则生成的文件不再使用
BUILDER_VERSION = "1"

INTEGER = "BIGINT"
DECIMAL = "DOUBLE"
TEXT = "VARCHAR"

_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")


class SemanticModelError(ValueError):
    """数据集的自述不合约定，建不出语义模型。消息进日志，不给模型看。"""


def valid_identifier(name: str) -> bool:
    return isinstance(name, str) and bool(_IDENTIFIER.fullmatch(name))


def physical_name(table: str) -> str:
    return PHYSICAL_PREFIX + table


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    type: str
    display_name: str | None = None
    unit: str | None = None


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[ColumnSpec, ...]
    row_count: int

    def column(self, name: str) -> ColumnSpec | None:
        return next((c for c in self.columns if c.name == name), None)


@dataclass(frozen=True)
class DatasetDescription:
    tables: tuple[TableSpec, ...]
    metrics: tuple[dict[str, object], ...] = ()
    metadata: dict[str, str] = field(default_factory=dict)

    def table(self, name: str) -> TableSpec | None:
        return next((t for t in self.tables if t.name == name), None)

    def validate(self) -> None:
        if not self.tables:
            raise SemanticModelError("dataset has no tables")
        seen: set[str] = set()
        for table in self.tables:
            lowered = table.name.lower()
            if not valid_identifier(table.name):
                raise SemanticModelError(
                    f"table name is not an identifier: {table.name}"
                )
            if lowered.startswith(PHYSICAL_PREFIX):
                raise SemanticModelError(
                    f"table name uses the reserved prefix: {table.name}"
                )
            if lowered in seen:
                raise SemanticModelError(f"table names differ only by case: {lowered}")
            seen.add(lowered)
            if not table.columns:
                raise SemanticModelError(f"table has no columns: {table.name}")
            columns: set[str] = set()
            for column in table.columns:
                if not valid_identifier(column.name):
                    raise SemanticModelError(
                        f"column name is not an identifier: {table.name}.{column.name}"
                    )
                if column.type not in (INTEGER, DECIMAL, TEXT):
                    raise SemanticModelError(
                        f"unsupported column type: {table.name}.{column.name}"
                    )
                if column.name.lower() in columns:
                    raise SemanticModelError(
                        f"column names differ only by case: {table.name}.{column.name}"
                    )
                columns.add(column.name.lower())
