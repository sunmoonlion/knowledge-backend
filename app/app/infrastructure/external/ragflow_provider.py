"""Convert RAGFlow HTTP vocabulary into the vendor-neutral data-plane Port."""

from __future__ import annotations

import hashlib
import math
from typing import Any

from app.application.ports.knowledge_provider import (
    ArtifactContent,
    ParseStatus,
    ProviderChunk,
    ProviderDataset,
    ProviderDocument,
    ProviderProtocolError,
    ProviderRetrievalResult,
)
from app.infrastructure.external.ragflow import (
    RAGFlowClient,
    RAGFlowRetrievalResult,
    _maybe_int,
    _normalise_run,
)
from core.config import Settings


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _score(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return 0.0
    return min(1.0, max(0.0, score)) if math.isfinite(score) else 0.0


def normalize_retrieval(result: RAGFlowRetrievalResult) -> ProviderRetrievalResult:
    return ProviderRetrievalResult(
        chunks=[
            ProviderChunk(
                id=_text(chunk.get("id") or chunk.get("chunk_id")),
                dataset_id=_text(chunk.get("dataset_id")),
                document_id=_text(chunk.get("document_id") or chunk.get("doc_id")),
                content=_text(
                    chunk.get("content") or chunk.get("content_with_weight")
                ).strip(),
                score=_score(chunk.get("similarity", chunk.get("score"))),
                term_similarity=(
                    _score(chunk["term_similarity"])
                    if chunk.get("term_similarity") is not None
                    else None
                ),
                vector_similarity=(
                    _score(chunk["vector_similarity"])
                    if chunk.get("vector_similarity") is not None
                    else None
                ),
            )
            for chunk in result.chunks
        ],
        total=result.total,
    )


def _document(value: dict[str, Any]) -> ProviderDocument:
    if not isinstance(value, dict) or any(
        not isinstance(value.get(key), str) or not value[key]
        for key in ("id", "dataset_id", "name")
    ):
        raise ProviderProtocolError("provider document identity is invalid")
    run = _normalise_run(value.get("run"))
    return ProviderDocument(
        id=value["id"],
        dataset_id=value["dataset_id"],
        name=value["name"],
        parse_status=ParseStatus(run) if run in ParseStatus else ParseStatus.UNKNOWN,
        chunk_count=_maybe_int(value.get("chunk_count")),
        token_count=_maybe_int(value.get("token_count")),
    )


class RAGFlowProvider:
    name = "ragflow"

    def __init__(
        self,
        settings: Settings,
        *,
        timeout_seconds: float = 30,
        client: RAGFlowClient | None = None,
    ) -> None:
        self._endpoint = (settings.ragflow_api_base or "").rstrip("/")
        self._client = client or RAGFlowClient(
            settings, timeout_seconds=timeout_seconds
        )

    async def close(self) -> None:
        await self._client.close()

    async def scope(self) -> str:
        tenant = (await self._client.get_tenant_models()).get("tenant_id")
        if not isinstance(tenant, str) or not tenant or not self._endpoint:
            raise ProviderProtocolError(
                "provider endpoint or tenant identity is missing"
            )
        # Byte-for-byte compatibility with existing upload intents and poll cursors.
        return hashlib.sha256(f"{self._endpoint}|{tenant}".encode()).hexdigest()

    async def find_datasets(self, name: str) -> list[ProviderDataset]:
        values = await self._client.find_datasets(name)
        if any(
            not isinstance(value.get(key), str) or not value[key]
            for value in values
            for key in ("id", "name")
        ):
            raise ProviderProtocolError("provider dataset identity is invalid")
        return [ProviderDataset(value["id"], value["name"]) for value in values]

    async def find_documents(
        self, dataset_id: str, filename: str
    ) -> list[ProviderDocument]:
        return [
            _document(value)
            for value in await self._client.find_documents(dataset_id, filename)
        ]

    async def get_document(self, dataset_id: str, document_id: str) -> ProviderDocument:
        return _document(await self._client.get_document(dataset_id, document_id))

    async def upload_document(
        self, dataset_id: str, artifact: ArtifactContent
    ) -> ProviderDocument:
        return _document(await self._client.upload_document(dataset_id, artifact))

    async def verify_document_content(
        self, dataset_id: str, document_id: str, *, size: int, sha256: str
    ) -> None:
        await self._client.verify_document_content(
            dataset_id, document_id, size=size, sha256=sha256
        )

    async def parse_document(self, dataset_id: str, document_id: str) -> None:
        await self._client.parse_document(dataset_id, document_id)

    async def retrieve(
        self,
        *,
        question: str,
        dataset_ids: list[str],
        document_ids: list[str],
        top_k: int,
    ) -> ProviderRetrievalResult:
        return normalize_retrieval(
            await self._client.retrieve(
                question=question,
                dataset_ids=dataset_ids,
                document_ids=document_ids,
                top_k=top_k,
            )
        )

    def ingestion_metadata(
        self, dataset: ProviderDataset, document: ProviderDocument
    ) -> dict[str, Any]:
        return {
            "ragflow_dataset_id": dataset.id,
            "ragflow_dataset_name": dataset.name,
            "ragflow_document_name": document.name,
            "ragflow_parse_status": document.parse_status.value,
            "ragflow_chunk_count": document.chunk_count,
            "ragflow_token_count": document.token_count,
        }
