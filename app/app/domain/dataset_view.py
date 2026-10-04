"""数据目录页面看到的样子（PRD/apps/knowledge.md 第三节）。

只有结构：表、列、口径、说明。没有任何一行数据，没有对象存储的位置。
这里只做整理，不读文件、不查库。
"""

from __future__ import annotations

from typing import Any

# 数据集里用来说明数据的表：页面把它们和装数据的表分开列
DICTIONARY_TABLES = frozenset(
    {
        "dataset_metadata",
        "field_dictionary",
        "metric_dictionary",
        "table_links",
        "table_keys",
        "reconciliation_rules",
    }
)
# 说明表里只是标识、时点的那几项：已经在摘要里，不当作说明再列一遍
_TECHNICAL = frozenset(
    {
        "data_snapshot_id",
        "dataset_export_version",
        "start_date",
        "end_date",
        "market",
        "security_code",
        "source_fingerprint",
    }
)
# 讲「数据从哪来、核对到什么程度」的几项，放在摘要里；其余的都是局限
_SOURCE_KEYS = ("statement_source", "official_source", "verified_range")
NOTE_WORDS = {
    "statement_source": "报表数据的来源",
    "official_source": "核对用的原文",
    "verified_range": "核对过的范围",
    "license_note": "使用范围",
    "unit_note": "单位",
    "interim_note": "中报与季报",
    "notice_date_note": "公告日",
    "restatement_note": "追溯调整",
    "accounting_note_lease": "租赁准则",
}


def matches(entry: dict[str, Any], query: str | None) -> bool:
    """按证券代码、数据集名或数据集标识找；不分大小写，包含就算。"""
    wanted = (query or "").strip().lower()
    if not wanted:
        return True
    return any(
        wanted in str(entry.get(field) or "").lower()
        for field in ("security_code", "title", "dataset")
    )


def note_label(key: str) -> str:
    if key in NOTE_WORDS:
        return NOTE_WORDS[key]
    if key.startswith("restatement_explanation_"):
        return f"{key.rsplit('_', 1)[-1]} 年追溯调整的原因"
    return key


def notes_of(metadata: dict[str, str]) -> tuple[list[dict[str, str]], list[dict]]:
    """把数据集自带的说明分成两份：来源说明、局限。"""

    def note(key: str) -> dict[str, str]:
        return {"key": key, "label": note_label(key), "text": metadata[key]}

    sources = [note(k) for k in _SOURCE_KEYS if metadata.get(k)]
    limitations = [
        note(k)
        for k in sorted(metadata)
        if k not in _TECHNICAL and k not in _SOURCE_KEYS and metadata[k]
    ]
    return sources, limitations


def table_view(
    table: dict[str, Any], labels: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    columns = []
    for column in table.get("columns") or []:
        name = str(column.get("name") or "")
        known = labels.get(name) or {}
        columns.append(
            {
                "name": name,
                "type": str(column.get("type") or ""),
                "label": known.get("display_name") or column.get("display_name"),
                "unit": known.get("unit") or column.get("unit"),
            }
        )
    name = str(table.get("name") or "")
    return {
        "name": name,
        "kind": "dictionary" if name in DICTIONARY_TABLES else "data",
        "row_count": table.get("row_count"),
        "columns": columns,
    }


def metric_view(row: dict[str, Any]) -> dict[str, Any]:
    tables = [
        str(t)
        for t in dict.fromkeys((row.get("base_table"), row.get("source_table")))
        if t
    ]
    queryable = row.get("queryable")
    return {
        "name": str(row.get("metric_name") or ""),
        "label": row.get("display_name"),
        "description": row.get("description"),
        "expression": row.get("value_expression") or row.get("expression_hint"),
        "unit": row.get("unit"),
        "time_basis": row.get("time_basis"),
        "tables": tables,
        "applicable_when": row.get("applicable_when"),
        "reason_if_not": row.get("reason_if_not"),
        # 老的数据集没有这一栏：不知道就是空，不猜
        "queryable": None if queryable is None else str(queryable) in ("1", "True"),
    }


def detail_view(
    entry: dict[str, Any],
    *,
    tables: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
    metadata: dict[str, str],
    labels: dict[str, dict[str, dict[str, Any]]],
) -> dict[str, Any]:
    sources, limitations = notes_of(metadata)
    return {
        **entry,
        "sources": sources,
        "tables": [table_view(t, labels.get(str(t.get("name")), {})) for t in tables],
        "metrics": [metric_view(m) for m in metrics],
        "limitations": limitations,
    }
