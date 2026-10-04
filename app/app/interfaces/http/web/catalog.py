"""数据目录：有哪些数据集、每个数据集的表、列、口径与局限（F-KNOW-08 至 11）。

只读，只给结构。任何一行数据、对象存储的位置都不从这里出去。
"""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from app.application.services.call_rate import RateLimiter
from app.application.services.dataset_catalog import UnknownDataset
from app.application.services.dataset_pages import DatasetPages
from app.bootstrap.datasets import shared_dataset_pages
from app.domain.security import Principal
from app.interfaces.http.middleware.auth import get_web_current_user
from app.interfaces.schemas.catalog import CatalogDatasetRead, CatalogListRead
from core.config import get_settings

log = logging.getLogger(__name__)

router = APIRouter(prefix="/web/v1/catalog", tags=["Data catalog"])


def get_dataset_pages() -> DatasetPages:
    """测试用依赖覆盖换目录。"""
    return shared_dataset_pages()


@lru_cache(maxsize=1)
def _shared_rate() -> RateLimiter:
    return RateLimiter(get_settings().knowledge_catalog_rate_per_minute)


def get_catalog_rate() -> RateLimiter:
    """页面自己的一份计数，与工具的限流分开计。"""
    return _shared_rate()


WebUser = Annotated[Principal, Depends(get_web_current_user)]
Pages = Annotated[DatasetPages, Depends(get_dataset_pages)]
Rate = Annotated[RateLimiter, Depends(get_catalog_rate)]


def _within_rate(user: Principal, rate: RateLimiter) -> None:
    if not rate.allow(user.subject):
        raise HTTPException(status_code=429, detail="请求太频繁，稍后再试")


@router.get("/datasets", response_model=CatalogListRead)
async def datasets(
    user: WebUser,
    pages: Pages,
    rate: Rate,
    q: Annotated[str | None, Query(max_length=80)] = None,
) -> dict:
    """现有的数据集，与工具 `list_datasets` 列的是同一份。`q` 按代码或名字找。"""
    _within_rate(user, rate)
    try:
        return await pages.listing(q)
    except Exception:  # 登记表查不了等：内部细节不给页面
        log.exception("catalog_listing_failed")
        raise HTTPException(status_code=503, detail="数据目录暂时打不开") from None


@router.get("/datasets/{dataset}", response_model=CatalogDatasetRead)
async def dataset(
    dataset: Annotated[str, Path(max_length=80)],
    user: WebUser,
    pages: Pages,
    rate: Rate,
) -> dict:
    """一个数据集的摘要、表、列、口径、局限。"""
    _within_rate(user, rate)
    try:
        return await pages.detail(dataset)
    except UnknownDataset:
        raise HTTPException(status_code=404, detail="没有这个数据集") from None
    except Exception:  # 文件取不到、登记表查不了等
        log.exception("catalog_detail_failed dataset=%s", dataset)
        raise HTTPException(status_code=503, detail="这个数据集暂时打不开") from None
