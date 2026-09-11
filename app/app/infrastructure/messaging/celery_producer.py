"""Celery producer — Backend API 向 RabbitMQ 投递异步任务。"""

from __future__ import annotations

import logging
from functools import lru_cache

from celery.result import AsyncResult

from app.worker import celery_app, configure_celery, is_celery_configured
from core.config import get_settings

logger = logging.getLogger(__name__)


class CeleryNotConfiguredError(RuntimeError):
    pass


class CeleryProducer:
    def _ensure_ready(self) -> None:
        if not configure_celery():
            raise CeleryNotConfiguredError(
                "Celery broker not configured (set CELERY_BROKER_URL)"
            )

    @property
    def enabled(self) -> bool:
        if is_celery_configured():
            return True
        return configure_celery()

    def dispatch_ping(self) -> str:
        """投递 ping 任务，返回 Celery task_id。"""
        self._ensure_ready()
        from app.tasks.ping import ping

        options = self._delivery_options()
        async_result = ping.apply_async(**options)
        logger.info(
            "已投递 ping 任务 task_id=%s queue=%s",
            async_result.id,
            options["queue"],
        )
        return async_result.id

    @staticmethod
    def _delivery_options() -> dict[str, str]:
        queue = get_settings().celery_queue
        return {"queue": queue, "exchange": queue, "routing_key": queue}

    def get_task_result(self, task_id: str) -> AsyncResult:
        self._ensure_ready()
        return AsyncResult(task_id, app=celery_app)


@lru_cache
def get_celery_producer() -> CeleryProducer:
    return CeleryProducer()
