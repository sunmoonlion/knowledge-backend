from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class DatasetRegister(BaseModel):
    """来源方登记一个已经建好并通过质量检查的数据集版本。"""

    model_config = ConfigDict(extra="forbid")

    dataset_id: str = Field(min_length=3, max_length=80)
    data_version: str = Field(min_length=3, max_length=160)
    title: str = Field(min_length=1, max_length=200)
    security_code: str | None = Field(default=None, max_length=6)
    object: str = Field(description="s3://bucket/key", max_length=1200)
    object_version_id: str | None = Field(default=None, max_length=255)
    sha256: str = Field(min_length=64, max_length=64)
    size_bytes: int = Field(gt=0)
    start_date: str = Field(min_length=10, max_length=10)
    end_date: str = Field(min_length=10, max_length=10)
    source_app: str = Field(min_length=1, max_length=80)
    source_ref: str | None = Field(default=None, max_length=255)
    quality_passed: bool


class DatasetRead(BaseModel):
    dataset_id: str
    data_version: str
    title: str
    security_code: str | None
    sha256: str
    size_bytes: int
    start_date: str
    end_date: str
    source_app: str
    source_ref: str | None
    status: str
    registered_by: str
    registered_at: datetime

    model_config = ConfigDict(from_attributes=True)
