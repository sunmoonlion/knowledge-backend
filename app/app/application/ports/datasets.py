"""多数据集的端口：登记表与数据集文件。"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from app.domain.datasets import DatasetRegistration, RegisteredDataset


class DatasetRegistry(Protocol):
    async def register(
        self, registration: DatasetRegistration, *, registered_by: str
    ) -> RegisteredDataset:
        """登记一个版本并使它成为该数据集的现行版本。同一版本重复登记是幂等的。"""
        ...

    async def active(self) -> list[RegisteredDataset]: ...

    async def get_active(self, dataset_id: str) -> RegisteredDataset | None: ...


class DatasetHistory(Protocol):
    """登记表的全部版本，现行的与被取代的。只给管理端看。"""

    async def versions(self, dataset_id: str | None = None) -> list[RegisteredDataset]:
        """按数据集、再按登记时间从新到旧。给了数据集就只要它的。"""
        ...


class DatasetFiles(Protocol):
    def ensure(self, dataset: RegisteredDataset) -> Path:
        """返回本地可读的数据集文件；需要时取回并按 sha256 校验。"""
        ...


class DatasetFileState(Protocol):
    def fetched(self, dataset: RegisteredDataset) -> bool:
        """这个版本的文件是不是已经取回到本地。只看，不取。"""
        ...
