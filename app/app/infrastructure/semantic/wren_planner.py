"""引擎的唯一接触点（0009-semantic，F-SEM-01）。换版本、换引擎只动这个文件。

只用引擎的规划：把查询按语义模型改写成目标方言的 SQL。执行由我们自己做。
"""

from __future__ import annotations

import base64
import json
import logging
import threading
from typing import Any

from loguru import logger as engine_logger
from wren.config import WrenConfig
from wren.engine import WrenEngine
from wren.model.error import WrenError

from app.application.services.dataset_query import SqlRejected
from app.application.services.semantic_guard import DENIED_FUNCTIONS, ONE_QUERY
from app.infrastructure.semantic.duckdb_store import sanitize

log = logging.getLogger(__name__)
ENGINE = "wrenai-0.15.0"
# 引擎用自己的日志库往标准错误写，内容里可能有查询原文；我们的日志策略不收这些
engine_logger.disable("wren")


class WrenPlanner:
    def __init__(self, manifest: dict[str, Any]) -> None:
        encoded = base64.b64encode(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode()
        ).decode()
        self._engine = WrenEngine(
            encoded,
            "duckdb",
            {},  # 只做规划，不给引擎连接
            fallback=False,
            config=WrenConfig(strict_mode=True, denied_functions=DENIED_FUNCTIONS),
        )
        self._lock = threading.Lock()

    def plan(self, sql: str) -> str:
        try:
            with self._lock:
                return self._engine.dry_plan(sql)
        except WrenError as exc:
            code = getattr(getattr(exc, "error_code", None), "name", "")
            log.info("semantic_plan_rejected code=%s", code)
            if code == "MODEL_NOT_FOUND":
                raise SqlRejected(
                    "the query refers to a table that does not exist in this "
                    "dataset; call describe_schema"
                ) from None
            if code in ("BLOCKED_STATEMENT", "BLOCKED_FUNCTION"):
                raise SqlRejected(ONE_QUERY) from None
            raise SqlRejected(
                f"the query could not be planned: {sanitize(_message(exc))}"
            ) from None


def _message(exc: WrenError) -> str:
    text = str(getattr(exc, "message", "") or exc)
    # 引擎的消息形如「[CODE] 说明 phase=…」；只留说明
    if text.startswith("[") and "]" in text:
        text = text.split("]", 1)[1]
    return text.split(" phase=", 1)[0].strip()
