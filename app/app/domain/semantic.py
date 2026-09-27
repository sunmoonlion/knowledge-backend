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
class LinkSpec:
    """两张表的行怎么对上。on_columns 是两边同名的字段。"""

    name: str
    from_table: str
    to_table: str
    cardinality: str
    on_columns: tuple[str, ...]


@dataclass(frozen=True)
class KeySpec:
    """一张表的一行由哪些字段确定，再带哪些字段才看得懂这一行。"""

    table: str
    key_columns: tuple[str, ...]
    label_columns: tuple[str, ...]

    @property
    def columns(self) -> tuple[str, ...]:
        return self.key_columns + self.label_columns


@dataclass(frozen=True)
class MetricSpec:
    """一个可以按名查询的口径。表达式已经校验过，字段都带上了表名。"""

    name: str
    display_name: str
    unit: str
    description: str
    base_table: str
    value_sql: str
    applicable_sql: str | None
    reason_if_not: str | None
    linked_tables: tuple[str, ...]
    formula: str
    applicable_when: str | None


@dataclass(frozen=True)
class DatasetDescription:
    tables: tuple[TableSpec, ...]
    metrics: tuple[dict[str, object], ...] = ()  # 口径表的原样内容
    metadata: dict[str, str] = field(default_factory=dict)
    links: tuple[LinkSpec, ...] = ()
    keys: tuple[KeySpec, ...] = ()

    def table(self, name: str) -> TableSpec | None:
        return next((t for t in self.tables if t.name == name), None)

    def key(self, table: str) -> KeySpec | None:
        return next((k for k in self.keys if k.table == table), None)

    def link(self, from_table: str, to_table: str) -> LinkSpec | None:
        return next(
            (
                link
                for link in self.links
                if link.from_table == from_table and link.to_table == to_table
            ),
            None,
        )

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
        self._validate_links_and_keys()

    def _has(self, table: str, columns: tuple[str, ...]) -> bool:
        spec = self.table(table)
        return (
            spec is not None
            and bool(columns)
            and all(spec.column(c) is not None for c in columns)
        )

    def _validate_links_and_keys(self) -> None:
        for link in self.links:
            if link.cardinality not in ("one_to_one", "many_to_one"):
                raise SemanticModelError(f"unsupported link kind: {link.name}")
            if link.from_table == link.to_table:
                raise SemanticModelError(f"a table cannot link to itself: {link.name}")
            for table in (link.from_table, link.to_table):
                if not self._has(table, link.on_columns):
                    raise SemanticModelError(
                        f"link refers to a missing table or column: {link.name}"
                    )
        seen: set[tuple[str, str]] = set()
        for link in self.links:
            pair = (link.from_table, link.to_table)
            if pair in seen:
                raise SemanticModelError(f"two links between the same tables: {pair}")
            seen.add(pair)
        tables: set[str] = set()
        for key in self.keys:
            if key.table in tables:
                raise SemanticModelError(f"two keys for one table: {key.table}")
            tables.add(key.table)
            if not self._has(key.table, key.key_columns) or (
                key.label_columns and not self._has(key.table, key.label_columns)
            ):
                raise SemanticModelError(
                    f"key refers to a missing table or column: {key.table}"
                )
            if len(set(key.columns)) != len(key.columns):
                raise SemanticModelError(f"key lists a column twice: {key.table}")
