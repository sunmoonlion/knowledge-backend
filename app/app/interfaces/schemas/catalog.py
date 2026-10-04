"""数据目录页面的返回（PRD/apps/knowledge.md 第五节）。只有结构，没有数据。"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class CatalogEntryRead(BaseModel):
    dataset: str
    title: str
    security_code: str | None = None
    data_version: str
    start_date: str | None = None
    end_date: str | None = None
    default: bool
    # 这个版本登记进来的时间。默认数据集不经登记，没有
    updated_at: datetime | None = None


class CatalogListRead(BaseModel):
    # 多数据集开没开：没开时目录里只有默认数据集
    registry_enabled: bool
    # 不管搜没搜，现在一共有几个
    total: int
    datasets: list[CatalogEntryRead]


class NoteRead(BaseModel):
    key: str
    label: str
    text: str


class ColumnRead(BaseModel):
    name: str
    type: str
    label: str | None = None
    unit: str | None = None


class TableRead(BaseModel):
    name: str
    # data：装数据的表；dictionary：数据集自带的说明表
    kind: str
    row_count: int | None = None
    columns: list[ColumnRead]


class MetricRead(BaseModel):
    name: str
    label: str | None = None
    description: str | None = None
    expression: str | None = None
    unit: str | None = None
    time_basis: str | None = None
    tables: list[str]
    applicable_when: str | None = None
    reason_if_not: str | None = None
    queryable: bool | None = None


class CatalogDatasetRead(CatalogEntryRead):
    sources: list[NoteRead]
    tables: list[TableRead]
    metrics: list[MetricRead]
    limitations: list[NoteRead]


# ---------------- 管理端：登记表 ----------------
class DatasetVersionRead(BaseModel):
    dataset_id: str
    data_version: str
    title: str
    security_code: str | None = None
    status: str
    start_date: str
    end_date: str
    sha256: str
    size_bytes: int
    # 存放位置：只有管理端看得到
    bucket: str
    object_key: str
    object_version_id: str | None = None
    source_app: str
    source_ref: str | None = None
    registered_by: str
    registered_at: datetime
    # 被取代的版本：它被取代的时间
    superseded_at: datetime | None = None
    # 文件取回到本地没有。取回时按校验值核对过才算取回
    fetched: bool


class RegisteredDatasetRead(BaseModel):
    dataset_id: str
    title: str
    security_code: str | None = None
    # 现行版本。一个数据集的版本全被取代的情况不会有，留空只为稳妥
    current: DatasetVersionRead | None = None
    version_count: int


class RegistryRead(BaseModel):
    enabled: bool
    datasets: list[RegisteredDatasetRead]


class DatasetVersionsRead(BaseModel):
    dataset_id: str
    versions: list[DatasetVersionRead]
