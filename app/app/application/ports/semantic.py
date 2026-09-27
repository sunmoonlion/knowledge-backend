"""语义层的端口（0009-semantic）。应用层只认这里的协议；引擎与库在基础设施层。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class QueryResult:
    columns: list[str]
    rows: list[dict[str, Any]]
    truncated: bool


class SqlPlanner(Protocol):
    def plan(self, sql: str) -> str:
        """按语义模型把查询规划成可执行的 SQL。

        查询不合规或引用了语义模型以外的东西时抛 SqlRejected，消息可以给模型看。
        """
        ...


class SqlExecutor(Protocol):
    def execute(self, planned_sql: str, *, limit: int, timeout_ms: int) -> QueryResult:
        """只读执行已规划的 SQL；最多取回 limit 行，多出的只标记截断。

        超时要中断查询。出错抛 SqlRejected，消息里不得有物理表名与文件路径。
        """
        ...
