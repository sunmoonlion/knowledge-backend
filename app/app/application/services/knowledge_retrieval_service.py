from __future__ import annotations

import hashlib
import uuid
from collections import defaultdict
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.errors.exceptions import (
    BadGatewayError,
    ForbiddenError,
    GatewayTimeoutError,
    ServiceUnavailableError,
)
from app.domain.security import Principal
from app.infrastructure.external.ragflow import (
    RAGFlowClient,
    RAGFlowError,
    RAGFlowProtocolError,
    RAGFlowRetrievalResult,
    RAGFlowTimeoutError,
)
from app.infrastructure.models.knowledge import KnowledgeDocument, KnowledgeDocumentVersion
from app.interfaces.schemas.retrieval import (
    Evidence,
    KnowledgeRetrievalRequest,
    KnowledgeRetrievalResponse,
    ProviderMetadata,
)
from core.config import get_settings


CHUNK_NAMESPACE = uuid.UUID("66b3a2ca-6880-4233-af81-9b8905672125")
EVIDENCE_NAMESPACE = uuid.UUID("7d9de8d6-cace-42e4-9477-405dce64594c")


async def retrieve_knowledge(
    session: AsyncSession,
    payload: KnowledgeRetrievalRequest,
    *,
    service_principal: Principal,
) -> KnowledgeRetrievalResponse:
    settings = get_settings()
    requested_datasets = set(payload.dataset_keys)
    if not settings.retrieval_datasets or not requested_datasets.issubset(
        settings.retrieval_datasets
    ):
        raise ForbiddenError("retrieval dataset is not authorized")
    if payload.security_context.tenant_id != settings.retrieval_default_tenant_id:
        raise ForbiddenError("retrieval tenant is not authorized")
    if settings.retrieval_auth_required_scope not in service_principal.scopes:
        raise ForbiddenError("retrieval service relation is not authorized")

    versions = await _eligible_versions(session, payload)
    tenant_scope = f"tenant:{payload.security_context.tenant_id}"
    versions = [version for version in versions if tenant_scope in set(version.access_scope or [])]
    if not versions:
        return KnowledgeRetrievalResponse(
            retrieval_id=uuid.uuid4(),
            request_id=payload.request_id,
            evidence=[],
            total_candidates=0,
            truncated=False,
        )
    if not settings.ragflow_enabled:
        raise ServiceUnavailableError("knowledge retrieval provider is not configured")

    provider_dataset_ids = sorted({version.provider_dataset_id for version in versions})
    provider_document_ids = sorted({version.provider_document_id for version in versions})
    client = RAGFlowClient(
        settings,
        timeout_seconds=settings.retrieval_provider_timeout_seconds,
    )
    try:
        result = await client.retrieve(
            question=payload.query,
            dataset_ids=provider_dataset_ids,
            document_ids=provider_document_ids,
            top_k=payload.top_k,
        )
    except RAGFlowTimeoutError as exc:
        raise GatewayTimeoutError("knowledge retrieval provider timed out") from exc
    except RAGFlowProtocolError as exc:
        raise BadGatewayError("knowledge retrieval provider returned an invalid response") from exc
    except RAGFlowError as exc:
        raise ServiceUnavailableError("knowledge retrieval provider is unavailable") from exc
    finally:
        await client.close()

    return _assemble_response(payload, result, versions)


async def _eligible_versions(
    session: AsyncSession,
    payload: KnowledgeRetrievalRequest,
) -> list[KnowledgeDocumentVersion]:
    query = (
        select(KnowledgeDocumentVersion)
        .join(
            KnowledgeDocument,
            KnowledgeDocument.id == KnowledgeDocumentVersion.knowledge_document_id,
        )
        .where(
            KnowledgeDocument.status == "active",
            KnowledgeDocumentVersion.status == "indexed",
            KnowledgeDocumentVersion.provider == "ragflow",
            KnowledgeDocumentVersion.dataset_key.in_(payload.dataset_keys),
        )
    )
    if payload.filters.source_document_ids:
        query = query.where(
            KnowledgeDocumentVersion.source_document_id.in_(
                payload.filters.source_document_ids
            )
        )
    if payload.filters.source_document_version_ids:
        query = query.where(
            KnowledgeDocumentVersion.source_document_version_id.in_(
                payload.filters.source_document_version_ids
            )
        )
    result = await session.execute(query)
    return list(result.scalars().all())


def _assemble_response(
    payload: KnowledgeRetrievalRequest,
    result: RAGFlowRetrievalResult,
    versions: list[KnowledgeDocumentVersion],
) -> KnowledgeRetrievalResponse:
    by_binding = {
        (version.provider_dataset_id, version.provider_document_id): version
        for version in versions
    }
    by_document: dict[str, list[KnowledgeDocumentVersion]] = defaultdict(list)
    for version in versions:
        by_document[version.provider_document_id].append(version)

    mapped: list[tuple[dict[str, Any], KnowledgeDocumentVersion]] = []
    for chunk in result.chunks:
        provider_document_id = _text(chunk.get("document_id") or chunk.get("doc_id"))
        provider_dataset_id = _text(chunk.get("dataset_id"))
        if not provider_document_id:
            continue
        version = by_binding.get((provider_dataset_id, provider_document_id))
        if version is None:
            candidates = by_document.get(provider_document_id, [])
            if len(candidates) == 1:
                version = candidates[0]
        if version is not None:
            mapped.append((chunk, version))

    evidence: list[Evidence] = []
    remaining_budget = payload.token_budget
    item_was_truncated = False
    for chunk, version in mapped:
        if len(evidence) >= payload.top_k or remaining_budget <= 0:
            break
        content = _chunk_content(chunk)
        if not content:
            continue
        truncated = len(content) > remaining_budget
        if truncated:
            content = content[:remaining_budget].rstrip() or content[:remaining_budget]
            item_was_truncated = True
        token_estimate = len(content)
        if token_estimate <= 0:
            continue
        remaining_budget -= token_estimate
        provider_chunk_id = _text(chunk.get("id") or chunk.get("chunk_id"))
        fingerprint = provider_chunk_id or hashlib.sha256(content.encode("utf-8")).hexdigest()
        chunk_id = uuid.uuid5(CHUNK_NAMESPACE, f"{version.id}:{fingerprint}")
        evidence_id = uuid.uuid5(EVIDENCE_NAMESPACE, f"{version.id}:{fingerprint}")
        evidence.append(
            Evidence(
                evidence_id=evidence_id,
                knowledge_document_id=version.knowledge_document_id,
                knowledge_document_version_id=version.id,
                chunk_id=chunk_id,
                content=content,
                score=_score(chunk.get("similarity", chunk.get("score"))),
                rank=len(evidence) + 1,
                title=version.title,
                source_uri=_safe_source_uri(version.source_uri),
                source_document_id=version.source_document_id,
                source_document_version_id=version.source_document_version_id,
                content_hash=version.content_hash,
                token_estimate=token_estimate,
                truncated=truncated,
                access_scope=sorted(set(version.access_scope or [])),
                provider_metadata=ProviderMetadata(
                    term_similarity=_optional_score(chunk.get("term_similarity")),
                    vector_similarity=_optional_score(chunk.get("vector_similarity")),
                ),
            )
        )

    return KnowledgeRetrievalResponse(
        retrieval_id=uuid.uuid4(),
        request_id=payload.request_id,
        evidence=evidence,
        total_candidates=len(mapped),
        truncated=(
            item_was_truncated
            or len(evidence) < len(mapped)
            or result.total > len(result.chunks)
        ),
    )


def _chunk_content(chunk: dict[str, Any]) -> str:
    value = chunk.get("content") or chunk.get("content_with_weight")
    return value.strip() if isinstance(value, str) else ""


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _score(value: Any) -> float:
    try:
        return min(1.0, max(0.0, float(value)))
    except (TypeError, ValueError):
        return 0.0


def _optional_score(value: Any) -> float | None:
    if value is None:
        return None
    return _score(value)


def _safe_source_uri(value: str | None) -> str | None:
    if not value or len(value) > 8192:
        return None
    parts = urlsplit(value)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        return None
    if parts.username or parts.password:
        return None
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
