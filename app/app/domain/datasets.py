"""登记的数据集（0008-info 段三）。

数据集文件由来源方建好，知识服务只登记、取用、提供查询。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

_DATASET_ID = re.compile(r"^[a-z0-9][a-z0-9-]{2,79}$")
_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,159}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_SECURITY = re.compile(r"^\d{6}$")
MAX_DATASET_BYTES = 512 * 1024 * 1024

ACTIVE = "active"
SUPERSEDED = "superseded"


class InvalidDatasetRegistration(ValueError):
    """登记请求不合规。消息可以直接返回给调用方。"""


class DatasetVersionConflict(ValueError):
    """同一个版本已经登记过，但内容或位置不同。"""


@dataclass(frozen=True)
class DatasetRegistration:
    dataset_id: str
    data_version: str
    title: str
    bucket: str
    object_key: str
    sha256: str
    size_bytes: int
    start_date: str
    end_date: str
    source_app: str
    source_ref: str | None = None
    security_code: str | None = None
    object_version_id: str | None = None

    def validate(self, *, allowed_buckets: frozenset[str]) -> None:
        def require(ok: bool, what: str) -> None:
            if not ok:
                raise InvalidDatasetRegistration(what)

        require(bool(_DATASET_ID.fullmatch(self.dataset_id)), "dataset_id is invalid")
        require(bool(_VERSION.fullmatch(self.data_version)), "data_version is invalid")
        require(
            self.data_version.startswith(self.dataset_id + "-"),
            "data_version must start with the dataset_id",
        )
        require(0 < len(self.title.strip()) <= 200, "title is required")
        require(bool(_SHA256.fullmatch(self.sha256)), "sha256 is invalid")
        require(0 < self.size_bytes <= MAX_DATASET_BYTES, "size_bytes is out of range")
        require(bool(_BUCKET.fullmatch(self.bucket)), "bucket is invalid")
        require(self.bucket in allowed_buckets, "bucket is not allowed for datasets")
        parts = self.object_key.split("/")
        require(
            0 < len(self.object_key) <= 1024
            and not self.object_key.startswith("/")
            and all(p not in ("", ".", "..") for p in parts)
            and all(ord(c) > 32 and ord(c) != 127 for c in self.object_key)
            and "\\" not in self.object_key,
            "object_key is invalid",
        )
        require(
            bool(_DATE.fullmatch(self.start_date))
            and bool(_DATE.fullmatch(self.end_date))
            and self.start_date <= self.end_date,
            "start_date and end_date are invalid",
        )
        require(
            0 < len(self.source_app) <= 80 and self.source_app.isascii(),
            "source_app is invalid",
        )
        require(
            self.security_code is None or bool(_SECURITY.fullmatch(self.security_code)),
            "security_code is invalid",
        )


@dataclass(frozen=True)
class RegisteredDataset:
    dataset_id: str
    data_version: str
    title: str
    bucket: str
    object_key: str
    object_version_id: str | None
    sha256: str
    size_bytes: int
    start_date: str
    end_date: str
    security_code: str | None
    source_app: str
    source_ref: str | None
    status: str
    registered_by: str
    registered_at: datetime

    def summary(self) -> dict[str, object]:
        return {
            "dataset": self.dataset_id,
            "title": self.title,
            "security_code": self.security_code,
            "data_version": self.data_version,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }
