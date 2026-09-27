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


class DatasetFiles(Protocol):
    def ensure(self, dataset: RegisteredDataset) -> Path:
        """返回本地可读的数据集文件；需要时取回并按 sha256 校验。"""
        ...
