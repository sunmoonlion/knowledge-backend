from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ArtifactRef(BaseModel):
    artifact_type: str | None = None
    bucket: str | None = None
    object_key: str | None = None
    uri: str | None = None
    content_type: str | None = None
    sha256: str | None = None
    size_bytes: int | None = None
    metadata: dict = Field(default_factory=dict)


class KnowledgeIngestionCreate(BaseModel):
    source_app: str = Field(default="info-app", min_length=1, max_length=80)
    source_document_id: uuid.UUID
    source_document_version_id: uuid.UUID
    source_artifact_refs: list[ArtifactRef] = Field(default_factory=list)
    title: str | None = None
    canonical_url: str | None = None
    source_name: str | None = None
    content_hash: str | None = Field(default=None, max_length=128)
    metadata: dict = Field(default_factory=dict)
    target_dataset: str | None = Field(default=None, max_length=255)
    profile_key: str = Field(default="markdown", min_length=1, max_length=80)
    idempotency_key: str | None = Field(default=None, max_length=255)


class KnowledgeIngestionStatusUpdate(BaseModel):
    status: str = Field(min_length=1, max_length=30)
    last_error: str | None = None
    metadata: dict = Field(default_factory=dict)
    knowledge_document_id: str | None = Field(default=None, max_length=255)
    ragflow_document_id: str | None = Field(default=None, max_length=255)


class KnowledgeIngestionRetryRequest(BaseModel):
    force: bool = False
    reason: str | None = Field(default=None, max_length=500)


class RAGFlowConfigCheckRead(BaseModel):
    enabled: bool
    reachable: bool
    has_default_embedding: bool
    ready: bool
    issues: list[str]
    details: dict = Field(default_factory=dict)


class KnowledgeIngestionRead(BaseModel):
    id: uuid.UUID
    source_app: str
    source_document_id: uuid.UUID
    source_document_version_id: uuid.UUID
    target_dataset: str | None
    profile_key: str
    idempotency_key: str
    title: str | None
    canonical_url: str | None
    source_name: str | None
    content_hash: str | None
    source_artifact_refs: list[dict]
    metadata_json: dict
    payload: dict
    status: str
    last_error: str | None
    status_history: list[dict]
    knowledge_document_id: str | None
    ragflow_document_id: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)
