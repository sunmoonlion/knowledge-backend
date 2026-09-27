"""交给引擎之前的检查（0009-semantic，SEM-04）。纯函数。"""

from __future__ import annotations

import pytest

from app.application.services.dataset_query import SqlRejected
from app.application.services.semantic_guard import DENIED_FUNCTIONS, check


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "select fiscal_year, operate_income from income_statement where fiscal_year>1",
        "SELECT * FROM income_statement;",
        "  SELECT * FROM income_statement ;  ",
        "WITH t AS (SELECT region FROM order_performance) SELECT COUNT(*) FROM t",
        "SELECT a.x FROM t1 a JOIN t2 b ON a.id = b.id",
        "SELECT x FROM t1 UNION ALL SELECT x FROM t2",
        "(SELECT 1)",
        "SELECT * FROM (SELECT fiscal_year FROM income_statement) s WHERE 1=1",
        "SELECT LAG(v) OVER (ORDER BY y) FROM t",
        "SELECT '; DROP TABLE x' AS text_with_a_semicolon",
        "SELECT 'read_csv' AS name, 'ds.main.phys_x' AS another",
        "SELECT income_statement.fiscal_year FROM income_statement",
        "WITH phys_like AS (SELECT 1 AS n) SELECT n FROM phys_like",
        "SELECT CASE WHEN x > 0 THEN y / x END AS r FROM t",
        "SELECT ROUND(1.0 * a / NULLIF(b, 0), 6) FROM t -- trailing comment",
    ],
)
def test_queries_pass(sql):
    body = check(sql)
    assert body and not body.endswith(";")


def test_the_query_is_returned_as_written():
    assert check(" SELECT  a ,b FROM t ; ") == "SELECT  a ,b FROM t"


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM income_statement",
        "INSERT INTO income_statement (fiscal_year) VALUES (1)",
        "UPDATE income_statement SET fiscal_year = 1",
        "CREATE TABLE evil AS SELECT 1",
        "DROP TABLE income_statement",
        "ALTER TABLE income_statement ADD COLUMN x INT",
        "TRUNCATE income_statement",
        "ATTACH DATABASE '/tmp/x.db' AS x",
        "DETACH ds",
        "PRAGMA database_list",
        "COPY (SELECT 1) TO '/tmp/out.csv'",
        "COPY income_statement TO '/tmp/out.csv'",
        "INSTALL httpfs",
        "LOAD httpfs",
        "SET enable_external_access=true",
        "USE ds",
        "EXPLAIN SELECT 1",
        "DESCRIBE income_statement",
        "SHOW TABLES",
        "SUMMARIZE income_statement",
        "CALL pragma_version()",
        "BEGIN",
        "SELECT 1; SELECT 2",
        "SELECT 1; DROP TABLE income_statement",
        "SELECT * INTO copied FROM income_statement",
        "CHECKPOINT",
        "VACUUM",
    ],
)
def test_anything_that_is_not_one_query_is_refused(sql):
    with pytest.raises(SqlRejected, match="exactly one read-only SELECT"):
        check(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM ds.main.phys_income_statement",
        "SELECT * FROM ds.main.income_statement",
        "SELECT * FROM main.income_statement",
        "SELECT * FROM ds.income_statement",
        'SELECT * FROM "ds"."main"."phys_income_statement"',
        "SELECT * FROM information_schema.tables",
        "SELECT * FROM pg_catalog.pg_tables",
        "SELECT * FROM system.main.duckdb_tables",
        "SELECT * FROM t WHERE x IN (SELECT x FROM main.other)",
        "WITH c AS (SELECT * FROM ds.main.phys_x) SELECT * FROM c",
        "SELECT a.* FROM income_statement a JOIN ds.main.phys_cash_flow b ON 1=1",
    ],
)
def test_qualified_table_names_are_refused(sql):
    with pytest.raises(SqlRejected, match="qualified table names are not allowed"):
        check(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM phys_income_statement",
        "SELECT * FROM PHYS_income_statement",
        'SELECT * FROM "phys_income_statement"',
        "SELECT COUNT(*) FROM income_statement a, phys_cash_flow b",
        "SELECT (SELECT MAX(x) FROM phys_cash_flow) AS m",
    ],
)
def test_physical_tables_are_unknown(sql):
    with pytest.raises(SqlRejected, match="unknown table"):
        check(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT * FROM read_text('/etc/hostname')",
        "SELECT * FROM read_parquet('s3://bucket/x.parquet')",
        "SELECT * FROM read_json_auto('/tmp/x.json')",
        "SELECT * FROM glob('/etc/*')",
        "SELECT * FROM duckdb_secrets()",
        "SELECT * FROM duckdb_settings()",
        "SELECT * FROM generate_series(1, 3)",
        "SELECT * FROM range(10)",
        "SELECT * FROM sqlite_scan('/tmp/x.db', 't')",
    ],
)
def test_table_functions_are_refused(sql):
    with pytest.raises(SqlRejected, match="not allowed"):
        check(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM '/etc/passwd'",
        "SELECT * FROM '/tmp/data.parquet'",
        'SELECT * FROM "/tmp/data.csv"',
        "SELECT * FROM 'https://example.com/x.parquet'",
        'SELECT * FROM "income statement"',
        'SELECT * FROM "x;y"',
    ],
)
def test_a_table_name_must_be_a_plain_identifier(sql):
    with pytest.raises(SqlRejected, match="plain identifiers"):
        check(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT getenv('HOME')",
        "SELECT GETENV('HOME')",
        "SELECT current_setting('access_mode')",
        "SELECT x FROM t WHERE y = getenv('SECRET')",
        "SELECT (SELECT getenv('A')) AS v",
        "SELECT * FROM t ORDER BY getenv('A')",
    ],
)
def test_functions_that_read_the_environment_are_refused(sql):
    with pytest.raises(SqlRejected, match="is not allowed"):
        check(sql)


@pytest.mark.parametrize(
    "sql",
    ["", "   ", ";", None, 12, "SELEC 1 FRM", "SELECT ((", "EXPORT DATABASE '/tmp/x'"],
)
def test_what_is_not_a_query(sql):
    with pytest.raises(SqlRejected):
        check(sql)


def test_very_long_sql_is_refused():
    with pytest.raises(SqlRejected, match="longer than"):
        check("SELECT " + ", ".join(["1"] * 12000))


def test_deeply_nested_sql_does_not_crash_the_service():
    sql = "SELECT " + "(" * 3000 + "1" + ")" * 3000
    try:
        check(sql)
    except SqlRejected:
        pass


def test_the_deny_list_covers_every_way_to_read_outside_the_dataset():
    for name in ("read_csv", "read_parquet", "read_text", "glob", "getenv"):
        assert name in DENIED_FUNCTIONS
