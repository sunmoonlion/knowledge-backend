"""按口径名查询（0009-semantic 乙段）：口径定义的校验，与查询 SQL 的生成。纯函数。

口径的定义来自数据集自己（自述第二版）。知识服务把数据集当数据看，不当代码看：
定义里的表达式先按语法树校验，只许字段、常数、四则运算、比较、逻辑、CASE 与几个
取值函数；字段必须是基础表的，或者是与基础表有关系的表的。

现在只做逐行的口径：基础表的一行（例如一家公司的一个报告期）算出一个值。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from app.application.services.dataset_query import SqlRejected
from app.domain.semantic import (
    INTEGER,
    TEXT,
    DatasetDescription,
    KeySpec,
    MetricSpec,
    SemanticModelError,
    valid_identifier,
)

log = logging.getLogger(__name__)
DIALECT = "duckdb"
MAX_METRICS = 10
MAX_FILTERS = 10
MAX_IN_VALUES = 50
APPLICABLE_SUFFIX = "__applicable"
MISSING_INPUT = "所需数据缺失"
NOT_SUPPORTED = (
    "this dataset does not support querying metrics by name; "
    "use metric_definitions and run_sql"
)
_OPERATORS: dict[str, type[exp.Binary]] = {
    "eq": exp.EQ,
    "neq": exp.NEQ,
    "gt": exp.GT,
    "gte": exp.GTE,
    "lt": exp.LT,
    "lte": exp.LTE,
}
_ALLOWED = (
    exp.Column,
    exp.Identifier,
    exp.Literal,
    exp.Boolean,
    exp.Null,
    exp.Paren,
    exp.Neg,
    exp.Add,
    exp.Sub,
    exp.Mul,
    exp.Div,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.EQ,
    exp.NEQ,
    exp.And,
    exp.Or,
    exp.Not,
    exp.Is,
    exp.Case,
    exp.If,
    exp.Coalesce,
    exp.Nullif,
    exp.Abs,
    exp.Round,
)


def _text(row: dict[str, object], name: str) -> str | None:
    value = row.get(name)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _expression(
    source: str, base: str, description: DatasetDescription
) -> tuple[str, set[str]]:
    """校验一个表达式，给每个字段带上表名。返回改写后的 SQL 与用到的有关系的表。"""
    try:
        parsed = sqlglot.parse_one(source, into=exp.Condition, dialect=DIALECT)
    except (SqlglotError, RecursionError):
        raise SemanticModelError("expression cannot be parsed") from None
    linked: set[str] = set()
    for node in parsed.walk():
        if not isinstance(node, _ALLOWED):
            raise SemanticModelError(
                f"expression uses something that is not allowed: {type(node).__name__}"
            )
        if not isinstance(node, exp.Column):
            continue
        if node.args.get("db") or node.args.get("catalog"):
            raise SemanticModelError("expression uses a qualified table name")
        table = str(node.table or base)
        spec = description.table(table)
        if spec is None or spec.column(str(node.name)) is None:
            raise SemanticModelError(f"expression refers to an unknown column: {node}")
        if table != base:
            if description.link(base, table) is None:
                raise SemanticModelError(
                    f"expression refers to a table that is not linked: {table}"
                )
            linked.add(table)
        node.set("table", exp.to_identifier(table, quoted=True))
        node.set("this", exp.to_identifier(str(node.name), quoted=True))
    return parsed.sql(dialect=DIALECT), linked


def queryable_metrics(description: DatasetDescription) -> dict[str, MetricSpec]:
    """口径表里可以按名查询的那些。定义不合规的记日志、不提供，不影响别的口径。"""
    found: dict[str, MetricSpec] = {}
    for row in description.metrics:
        name = _text(row, "metric_name")
        if not name or row.get("queryable") not in (1, True):
            continue
        try:
            found[name] = _metric(name, row, description)
        except SemanticModelError as exc:
            log.error("semantic_metric_rejected metric=%s reason=%s", name, exc)
    return found


def _metric(
    name: str, row: dict[str, object], description: DatasetDescription
) -> MetricSpec:
    if not valid_identifier(name) or name.endswith(APPLICABLE_SUFFIX):
        raise SemanticModelError("metric name is not an identifier")
    if _text(row, "kind") != "row":
        raise SemanticModelError("only row-level metrics can be queried by name")
    base = _text(row, "base_table")
    formula = _text(row, "value_expression")
    if base is None or formula is None:
        raise SemanticModelError("metric has no base table or expression")
    key = description.key(base)
    if description.table(base) is None or key is None:
        raise SemanticModelError("base table is missing or has no key")
    if name in key.columns:
        raise SemanticModelError("metric name is also a column of the base table")
    value_sql, linked = _expression(formula, base, description)
    condition = _text(row, "applicable_when")
    applicable_sql = None
    if condition is not None:
        applicable_sql, more = _expression(condition, base, description)
        linked |= more
    return MetricSpec(
        name=name,
        display_name=_text(row, "display_name") or name,
        unit=_text(row, "unit") or "",
        description=_text(row, "description") or "",
        base_table=base,
        value_sql=value_sql,
        applicable_sql=applicable_sql,
        reason_if_not=_text(row, "reason_if_not"),
        linked_tables=tuple(sorted(linked)),
        formula=formula,
        applicable_when=condition,
    )


@dataclass(frozen=True)
class MetricQuery:
    sql: str
    metrics: tuple[MetricSpec, ...]
    key: KeySpec


def _column(table: str, name: str) -> exp.Column:
    return exp.column(name, table=table, quoted=True)


def _literal(value: Any, kind: str, column: str) -> exp.Expr:
    if kind == TEXT:
        if not isinstance(value, str) or len(value) > 200:
            raise SqlRejected(f"filter on {column} needs a text value")
        return exp.Literal.string(value)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise SqlRejected(f"filter on {column} needs a number")
    if kind == INTEGER and not isinstance(value, int):
        raise SqlRejected(f"filter on {column} needs a whole number")
    if isinstance(value, float) and value != value:  # NaN
        raise SqlRejected(f"filter on {column} needs a number")
    literal = exp.Literal.number(abs(value))
    return exp.Neg(this=literal) if value < 0 else literal


def _filter(item: Any, base: str, key: KeySpec, types: dict[str, str]) -> exp.Expr:
    if not isinstance(item, dict) or set(item) - {"column", "op", "value"}:
        raise SqlRejected("each filter is an object with column, op and value")
    column, op = item.get("column"), item.get("op")
    if column not in key.columns:
        raise SqlRejected(
            f"filters may use these columns only: {', '.join(key.columns)}"
        )
    kind = types[column]
    left = _column(base, column)
    if op in ("in", "not_in"):
        values = item.get("value")
        if not isinstance(values, list) or not 1 <= len(values) <= MAX_IN_VALUES:
            raise SqlRejected(
                f"filter {op} needs a list of 1 to {MAX_IN_VALUES} values"
            )
        chosen = exp.In(
            this=left, expressions=[_literal(v, kind, column) for v in values]
        )
        return exp.Not(this=chosen) if op == "not_in" else chosen
    if op not in _OPERATORS:
        raise SqlRejected(
            "filter op must be one of: " + ", ".join([*_OPERATORS, "in", "not_in"])
        )
    return _OPERATORS[op](
        this=left, expression=_literal(item.get("value"), kind, column)
    )


def build_query(
    description: DatasetDescription,
    available: dict[str, MetricSpec],
    *,
    metrics: Any,
    filters: Any = None,
    order_by: Any = None,
) -> MetricQuery:
    """把「口径名、筛选、排序」变成一条查询。每个基础表的行一行结果。"""
    if not available:
        raise SqlRejected(NOT_SUPPORTED)
    if (
        not isinstance(metrics, list)
        or not 1 <= len(metrics) <= MAX_METRICS
        or not all(isinstance(m, str) for m in metrics)
        or len(set(metrics)) != len(metrics)
    ):
        raise SqlRejected(
            f"metrics is a list of 1 to {MAX_METRICS} different metric names"
        )
    unknown = [m for m in metrics if m not in available]
    if unknown:
        raise SqlRejected(
            f"these metrics cannot be queried by name: {', '.join(unknown)}; "
            f"available: {', '.join(sorted(available))}"
        )
    chosen = tuple(available[m] for m in metrics)
    bases = sorted({m.base_table for m in chosen})
    if len(bases) != 1:
        raise SqlRejected(
            "these metrics are based on different tables "
            f"({', '.join(bases)}); query them separately"
        )
    base = bases[0]
    key = description.key(base)
    table = description.table(base)
    assert key is not None and table is not None  # 口径校验时已经确认
    types = {c.name: c.type for c in table.columns}

    select: list[exp.Expr] = [
        exp.alias_(_column(base, c), c, quoted=True) for c in key.columns
    ]
    for metric in chosen:
        value = sqlglot.parse_one(metric.value_sql, dialect=DIALECT)
        if metric.applicable_sql is None:
            applicable: exp.Expr = exp.true()
        else:
            condition = sqlglot.parse_one(metric.applicable_sql, dialect=DIALECT)
            applicable = exp.Coalesce(
                this=exp.Paren(this=condition), expressions=[exp.false()]
            )
            value = exp.Case(
                ifs=[exp.If(this=applicable.copy(), true=exp.Paren(this=value))]
            )
        select.append(exp.alias_(value, metric.name, quoted=True))
        select.append(
            exp.alias_(applicable, metric.name + APPLICABLE_SUFFIX, quoted=True)
        )

    query = exp.select(*select).from_(exp.to_table(base, quoted=True))
    for other in sorted({t for m in chosen for t in m.linked_tables}):
        link = description.link(base, other)
        assert link is not None
        on = exp.and_(
            *[
                exp.EQ(this=_column(base, c), expression=_column(other, c))
                for c in link.on_columns
            ]
        )
        query = query.join(exp.to_table(other, quoted=True), on=on, join_type="left")

    if filters is not None:
        if not isinstance(filters, list) or len(filters) > MAX_FILTERS:
            raise SqlRejected(f"filters is a list of at most {MAX_FILTERS} filters")
        for item in filters:
            query = query.where(_filter(item, base, key, types))

    query = query.order_by(*_order(order_by, base, key, metrics))
    return MetricQuery(query.sql(dialect=DIALECT), chosen, key)


def _order(order_by: Any, base: str, key: KeySpec, metrics: list[str]) -> list[Any]:
    if order_by is None:
        return [exp.Ordered(this=_column(base, c), desc=False) for c in key.key_columns]
    if not isinstance(order_by, list) or not 1 <= len(order_by) <= 5:
        raise SqlRejected("order_by is a list of 1 to 5 entries")
    ordered = []
    for item in order_by:
        if not isinstance(item, dict) or set(item) - {"by", "direction"}:
            raise SqlRejected("each order_by entry is an object with by and direction")
        by, direction = item.get("by"), item.get("direction", "asc")
        if direction not in ("asc", "desc"):
            raise SqlRejected("direction is asc or desc")
        if by in key.columns:
            target: exp.Expr = _column(base, by)
        elif by in metrics:
            target = exp.to_identifier(by, quoted=True)
        else:
            raise SqlRejected(
                "order_by may use the requested metrics and these columns: "
                + ", ".join(key.columns)
            )
        ordered.append(
            exp.Ordered(this=target, desc=direction == "desc", nulls_first=False)
        )
    # 末尾补上主键，结果的顺序才是确定的
    for column in key.key_columns:
        if all(i.get("by") != column for i in order_by):
            ordered.append(exp.Ordered(this=_column(base, column), desc=False))
    return ordered


def shape(rows: list[dict[str, Any]], query: MetricQuery) -> list[dict[str, Any]]:
    """每行：基础表的主键与说明字段，加上每个口径的值、是否适用、原因。"""
    shaped = []
    for row in rows:
        entry: dict[str, Any] = {c: row.get(c) for c in query.key.columns}
        values: dict[str, Any] = {}
        for metric in query.metrics:
            applicable = bool(row.get(metric.name + APPLICABLE_SUFFIX))
            value = row.get(metric.name) if applicable else None
            reason = None
            if not applicable:
                reason = metric.reason_if_not or "不适用"
            elif value is None:
                reason = MISSING_INPUT
            values[metric.name] = {
                "value": value,
                "unit": metric.unit,
                "applicable": applicable,
                "reason": reason,
            }
        entry["metrics"] = values
        shaped.append(entry)
    return shaped
