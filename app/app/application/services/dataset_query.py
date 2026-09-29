"""数据集查询的规则（0006「最小实例」）：只读 SQL、按名字找口径、结果的出处。

- 只读：单条 SELECT/WITH，禁 ATTACH/PRAGMA；行数封顶（F-KNOW-05 不整批下发）。
- 每个结果带来源、时点、数据版本（F-KNOW-02 进证据账）；
  口径表随数据版本（F-KNOW-07）。

读数据集文件的实现在基础设施层：只读 SQLite（`infrastructure/datasets/`）与语义层
（`infrastructure/semantic/`），两者都满足 `dataset_catalog.DatasetQueries`。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
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


def citation(info: DatasetInfo, **extra: Any) -> dict[str, Any]:
    return {
        "dataset_id": info.dataset_id,
        "data_version": info.data_version,
        "source": info.source,
        "as_of": info.end_date,
        "retrieved_at": datetime.now(UTC).isoformat(timespec="seconds"),
        **extra,
    }


def match_metrics(rows: list[dict[str, Any]], metric: str) -> list[dict[str, Any]]:
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


def guard_read_only(sql: str) -> str:
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
