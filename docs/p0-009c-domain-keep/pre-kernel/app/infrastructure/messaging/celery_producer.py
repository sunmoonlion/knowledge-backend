"""Celery producer — admin-backend API 向 RabbitMQ 投递异步任务。"""

from __future__ import annotations

import logging
from functools import lru_cache
import uuid

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

    def _delivery_options(self) -> dict[str, str]:
        queue = get_settings().celery_queue
        return {
            "queue": queue,
            "exchange": queue,
            "routing_key": queue,
        }

    def dispatch_ping(self) -> str:
        """投递 ping 任务，返回 Celery task_id。"""
        self._ensure_ready()
        from app.tasks.ping import ping

        options = self._delivery_options()
        async_result = ping.apply_async(**options)
        logger.info("已投递 ping 任务 task_id=%s queue=%s", async_result.id, options["queue"])
        return async_result.id

    def dispatch_knowledge_ingestion(self, ingestion_id: uuid.UUID) -> str:
        """投递 knowledge ingestion 处理任务，返回 Celery task_id。"""
        self._ensure_ready()
        from app.tasks.knowledge_ingestion import process_knowledge_ingestion

        async_result = process_knowledge_ingestion.apply_async(
            args=[str(ingestion_id)], **self._delivery_options()
        )
        logger.info(
            "已投递 process_knowledge_ingestion 任务 task_id=%s ingestion_id=%s queue=%s",
            async_result.id,
            ingestion_id,
            get_settings().celery_queue,
        )
        return async_result.id

    def get_task_result(self, task_id: str) -> AsyncResult:
        self._ensure_ready()
        return AsyncResult(task_id, app=celery_app)


@lru_cache
def get_celery_producer() -> CeleryProducer:
    return CeleryProducer()
