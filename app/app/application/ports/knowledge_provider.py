"""Knowledge data-plane boundary; no vendor HTTP shapes or database ownership.

Adapters must report ambiguous writes, not retry them. Exact lookup must reject
truncated results; content verification must validate bytes, not just a filename.
Dataset provisioning/deletion and automatic provider switching are NOT this Port.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol


class ProviderError(RuntimeError):
    pass


class ProviderTimeoutError(ProviderError):
    pass


class ProviderProtocolError(ProviderError):
    pass


class ProviderParseError(ProviderError):
    pass


class ProviderParseCancelledError(ProviderParseError):
    pass


class ProviderOutcomeUnknown(ProviderError):
    """No verified receipt: absence never authorizes another remote write."""


class ArtifactError(ProviderError):
    pass


class ParseStatus(StrEnum):
    # Values retain the existing persisted cursor vocabulary, not wire values.
    NOT_STARTED = "UNSTART"
    QUEUED = "SCHEDULE"
    RUNNING = "RUNNING"
    SUCCEEDED = "DONE"
    FAILED = "FAIL"
    CANCELLED = "CANCEL"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ArtifactContent:
    filename: str
    content: bytes
    content_type: str


@dataclass(frozen=True)
class ProviderDataset:
    id: str
    name: str


@dataclass(frozen=True)
class ProviderDocument:
    id: str
    dataset_id: str
    name: str
    parse_status: ParseStatus = ParseStatus.UNKNOWN
    chunk_count: int | None = None
    token_count: int | None = None


@dataclass(frozen=True)
class ProviderChunk:
    id: str
    dataset_id: str
    document_id: str
    content: str
    score: float
    term_similarity: float | None = None
    vector_similarity: float | None = None


@dataclass(frozen=True)
class ProviderRetrievalResult:
    chunks: list[ProviderChunk]
    total: int


@dataclass(frozen=True)
class ProviderIngestionResult:
    provider: str
    dataset_id: str
    dataset_name: str
    document_id: str
    document_name: str
    parse_status: str
    chunk_count: int | None
    token_count: int | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderDefinition:
    name: str
    enabled: bool
    endpoint_fingerprint: str
    parse_timeout_seconds: int
    parse_poll_interval_seconds: float


class KnowledgeProvider(Protocol):
    @property
    def name(self) -> str: ...

    async def close(self) -> None: ...

    async def scope(self) -> str:
        """Stable endpoint + verified remote tenant identity, never credentials."""
        ...

    async def find_datasets(self, name: str) -> list[ProviderDataset]: ...

    async def find_documents(
        self, dataset_id: str, filename: str
    ) -> list[ProviderDocument]: ...

    async def upload_document(
        self, dataset_id: str, artifact: ArtifactContent
    ) -> ProviderDocument: ...

    async def get_document(
        self, dataset_id: str, document_id: str
    ) -> ProviderDocument: ...

    async def verify_document_content(
        self, dataset_id: str, document_id: str, *, size: int, sha256: str
    ) -> None: ...

    async def parse_document(self, dataset_id: str, document_id: str) -> None: ...

    async def retrieve(
        self,
        *,
        question: str,
        dataset_ids: list[str],
        document_ids: list[str],
        top_k: int,
    ) -> ProviderRetrievalResult: ...

    def ingestion_metadata(
        self, dataset: ProviderDataset, document: ProviderDocument
    ) -> dict[str, Any]:
        """Opaque diagnostic projection; never used for authorization/state."""
        ...
