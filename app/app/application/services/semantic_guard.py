"""交给引擎之前的检查（0009-semantic，F-SEM-03）。按语法树判断，不按字符串。

引擎的严格模式只拿表的裸名去对照语义模型；带库名的引用会绕过它落到物理表上。
所以这里先拦：带库名或模式名的表引用、物理表、表函数、读文件与读环境的函数。
"""

from __future__ import annotations

from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from app.application.services.dataset_query import SqlRejected
from app.domain.semantic import PHYSICAL_PREFIX, valid_identifier

MAX_SQL_CHARS = 20000
DIALECT = "duckdb"
DENIED_FUNCTIONS = frozenset(
    {
        "read_text",
        "read_blob",
        "read_csv",
        "read_csv_auto",
        "read_json",
        "read_json_auto",
        "read_json_objects",
        "read_ndjson",
        "read_ndjson_auto",
        "read_parquet",
        "parquet_scan",
        "parquet_metadata",
        "parquet_schema",
        "sniff_csv",
        "glob",
        "sqlite_scan",
        "sqlite_attach",
        "postgres_scan",
        "mysql_scan",
        "iceberg_scan",
        "delta_scan",
        "getenv",
        "current_setting",
        "duckdb_secrets",
        "duckdb_settings",
        "duckdb_extensions",
        "duckdb_databases",
        "duckdb_tables",
        "duckdb_columns",
        "duckdb_views",
        "duckdb_functions",
        "which_secret",
        "query",
        "query_table",
    }
)
_QUERY = (exp.Select, exp.SetOperation)
_NOT_A_QUERY = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Command,
    exp.Copy,
    exp.Attach,
    exp.Detach,
    exp.Pragma,
    exp.Set,
    exp.Use,
    exp.Transaction,
    exp.Commit,
    exp.Rollback,
    exp.Into,
)
ONE_QUERY = "exactly one read-only SELECT (or WITH ... SELECT) statement is allowed"


def _function_name(node: Any) -> str:
    if isinstance(node, exp.Anonymous):
        return str(node.name or "").lower()
    if isinstance(node, exp.Func):
        return str(node.sql_name() or "").lower()
    return ""


def check(sql: str) -> str:
    """返回去掉结尾分号的查询；不合规抛 SqlRejected。"""
    if not isinstance(sql, str) or not sql.strip():
        raise SqlRejected("empty SQL")
    if len(sql) > MAX_SQL_CHARS:
        raise SqlRejected(f"SQL is longer than {MAX_SQL_CHARS} characters")
    try:
        statements = [s for s in sqlglot.parse(sql, dialect=DIALECT) if s is not None]
    except (SqlglotError, RecursionError):
        raise SqlRejected("the SQL could not be parsed") from None
    if len(statements) != 1:
        raise SqlRejected(ONE_QUERY)
    root = statements[0]
    while isinstance(root, exp.Paren | exp.Subquery):
        root = root.this
    if not isinstance(root, _QUERY):
        raise SqlRejected(ONE_QUERY)
    cte_names = {str(c.alias_or_name).lower() for c in root.find_all(exp.CTE)}
    for node in root.walk():
        if isinstance(node, _NOT_A_QUERY):
            raise SqlRejected(ONE_QUERY)
        if isinstance(node, exp.Table):
            _check_table(node, cte_names)
        name = _function_name(node)
        if name and name in DENIED_FUNCTIONS:
            raise SqlRejected(f"function {name} is not allowed")
    return sql.strip().rstrip(";").rstrip()


def _check_table(table: exp.Table, cte_names: set[str]) -> None:
    if not isinstance(table.this, exp.Identifier):
        raise SqlRejected("table functions are not allowed; query the tables by name")
    if table.args.get("catalog") or table.args.get("db"):
        raise SqlRejected(
            "qualified table names are not allowed; use the table name alone"
        )
    name = str(table.name or "")
    if not valid_identifier(name):
        # 引号里放文件路径会被库当成「直接读这个文件」
        raise SqlRejected("table names must be plain identifiers")
    if name.lower().startswith(PHYSICAL_PREFIX) and name.lower() not in cte_names:
        raise SqlRejected(f"unknown table: {name}; call describe_schema")
