from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass
from typing import Any

import httpx

from app.application.ports.knowledge_provider import (
    ArtifactContent as ArtifactContent,
)
from app.application.ports.knowledge_provider import (
    ProviderError,
    ProviderParseCancelledError,
    ProviderParseError,
    ProviderProtocolError,
    ProviderTimeoutError,
)
from core.config import Settings


class RAGFlowError(ProviderError):
    pass


class RAGFlowTimeoutError(RAGFlowError, ProviderTimeoutError):
    pass


class RAGFlowProtocolError(RAGFlowError, ProviderProtocolError):
    pass


class RAGFlowParseError(RAGFlowError, ProviderParseError):
    pass


class RAGFlowParseCancelledError(RAGFlowParseError, ProviderParseCancelledError):
    """解析被取消（RAGFlow run=CANCEL）。

    继承 RAGFlowParseError，因此沿用其可重试的错误分类；单独立类是为了在
    metadata 里与「解析失败」区分开——取消多为运维动作或 RAGFlow 侧重启，
    排查方向不同。
    """


@dataclass(frozen=True)
class RAGFlowIngestionResult:
    dataset_id: str
    dataset_name: str
    document_id: str
    document_name: str
    parse_status: str
    chunk_count: int | None
    token_count: int | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class RAGFlowConfigCheck:
    enabled: bool
    reachable: bool
    has_default_embedding: bool
    issues: list[str]
    details: dict[str, Any]


@dataclass(frozen=True)
class RAGFlowRetrievalResult:
    chunks: list[dict[str, Any]]
    total: int


class RAGFlowClient:
    def __init__(
        self,
        settings: Settings,
        client: httpx.AsyncClient | None = None,
        *,
        timeout_seconds: float = 30,
    ) -> None:
        if not settings.ragflow_api_base or not settings.ragflow_api_key:
            raise RAGFlowError("RAGFlow is not configured")
        base = settings.ragflow_api_base.rstrip("/")
        if not base.endswith("/api/v1"):
            base = f"{base}/api/v1"
        self._base = base
        self._api_key = settings.ragflow_api_key
        self._client = client or httpx.AsyncClient(timeout=timeout_seconds)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._api_key}"}

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = await self._client.request(
                method, f"{self._base}{path}", headers=self._headers(), **kwargs
            )
            response.raise_for_status()
        except httpx.TimeoutException as exc:
            raise RAGFlowTimeoutError("RAGFlow request timed out") from exc
        except httpx.HTTPError as exc:
            raise RAGFlowError(f"RAGFlow HTTP request failed: {exc}") from exc
        try:
            data = response.json()
        except ValueError as exc:
            raise RAGFlowProtocolError("RAGFlow response is not valid JSON") from exc
        if not isinstance(data, dict):
            raise RAGFlowProtocolError("RAGFlow response is not an object")
        if data.get("code") != 0:
            raise RAGFlowError(str(data.get("message") or data))
        return data

    async def list_datasets(self, page_size: int = 10) -> list[dict[str, Any]]:
        data = await self._request("GET", "/datasets", params={"page_size": page_size})
        datasets = data.get("data") or []
        return [item for item in datasets if isinstance(item, dict)]

    async def find_datasets(self, name: str) -> list[dict[str, Any]]:
        # v0.25.4 deliberately treats a missing exact name as an authorization
        # error. Enumerate authorized datasets; never reinterpret denial as empty.
        matches = []
        seen = 0
        for page in range(1, 101):
            data = await self._request(
                "GET",
                "/datasets",
                params={
                    "page": page,
                    "page_size": 100,
                    "orderby": "create_time",
                    "desc": "false",
                },
            )
            items = data.get("data")
            # The deployed response wrapper emits total_datasets, despite its
            # docstring describing total. Accept both explicit pagination fields.
            total = data.get("total_datasets", data.get("total"))
            if (
                not isinstance(items, list)
                or not all(isinstance(item, dict) for item in items)
                or not isinstance(total, int)
                or isinstance(total, bool)
                or total < 0
            ):
                raise RAGFlowProtocolError("invalid dataset lookup response")
            matches.extend(item for item in items if item.get("name") == name)
            seen += len(items)
            if seen >= total:
                return matches
            if not items:
                raise RAGFlowProtocolError("dataset lookup was truncated")
        raise RAGFlowProtocolError("dataset lookup exceeded reconciliation limit")

    async def create_dataset(self, name: str) -> dict[str, Any]:
        # Kept as a fail-closed compatibility entry, never an HTTP write.
        raise RAGFlowProtocolError("dataset provisioning is disabled in the data plane")

    async def find_documents(
        self, dataset_id: str, filename: str
    ) -> list[dict[str, Any]]:
        # `name` returns an API error on absence in v0.25.4; keywords supports an
        # empty result. Filter exact names locally and refuse incomplete queries.
        data = await self._request(
            "GET",
            f"/datasets/{dataset_id}/documents",
            params={"keywords": filename, "page_size": 100},
        )
        result = data.get("data")
        if not isinstance(result, dict) or not isinstance(result.get("docs"), list):
            raise RAGFlowProtocolError("invalid document lookup response")
        items = result["docs"]
        if not all(isinstance(item, dict) for item in items):
            raise RAGFlowProtocolError("invalid document lookup item")
        if result.get("total", len(items)) > len(items):
            raise RAGFlowProtocolError("document lookup was truncated")
        return [item for item in items if item.get("name") == filename]

    async def verify_document_content(
        self, dataset_id: str, document_id: str, *, size: int, sha256: str
    ) -> None:
        digest = hashlib.sha256()
        received = 0
        try:
            async with self._client.stream(
                "GET",
                f"{self._base}/datasets/{dataset_id}/documents/{document_id}",
                headers=self._headers(),
            ) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes():
                    received += len(chunk)
                    if received > size:
                        raise RAGFlowProtocolError(
                            "remote document exceeds expected size"
                        )
                    digest.update(chunk)
        except httpx.HTTPError as exc:
            raise RAGFlowError("remote document verification failed") from exc
        if received != size or not hmac.compare_digest(digest.hexdigest(), sha256):
            raise RAGFlowProtocolError(
                "remote document content does not match source version"
            )

    async def get_tenant_models(self) -> dict[str, Any]:
        data = await self._request("GET", "/users/me/models")
        tenant = data.get("data") or {}
        if not isinstance(tenant, dict):
            raise RAGFlowError("RAGFlow tenant model response is not an object")
        return tenant

    async def upload_document(
        self, dataset_id: str, artifact: ArtifactContent
    ) -> dict[str, Any]:
        files = {
            "file": (
                artifact.filename,
                artifact.content,
                artifact.content_type or "text/plain",
            )
        }
        data = await self._request(
            "POST", f"/datasets/{dataset_id}/documents", files=files
        )
        documents = data.get("data") or []
        if not documents:
            raise RAGFlowError("RAGFlow upload returned no document")
        return documents[0]

    async def parse_document(self, dataset_id: str, document_id: str) -> None:
        await self._request(
            "POST",
            f"/datasets/{dataset_id}/documents/parse",
            json={"document_ids": [document_id]},
        )

    async def get_document(self, dataset_id: str, document_id: str) -> dict[str, Any]:
        data = await self._request(
            "GET", f"/datasets/{dataset_id}/documents", params={"id": document_id}
        )
        docs = (data.get("data") or {}).get("docs") or []
        for doc in docs:
            if doc.get("id") == document_id:
                return doc
        raise RAGFlowError(f"RAGFlow document not found: {document_id}")

    async def retrieve(
        self,
        *,
        question: str,
        dataset_ids: list[str],
        document_ids: list[str],
        top_k: int,
    ) -> RAGFlowRetrievalResult:
        data = await self._request(
            "POST",
            "/retrieval",
            json={
                "question": question,
                "dataset_ids": dataset_ids,
                "document_ids": document_ids,
                "page": 1,
                "page_size": top_k,
                "top_k": max(top_k, 32),
                "similarity_threshold": 0.0,
                "vector_similarity_weight": 0.3,
                "keyword": False,
                "highlight": False,
            },
        )
        result = data.get("data")
        if not isinstance(result, dict):
            raise RAGFlowProtocolError("RAGFlow retrieval data is not an object")
        chunks = result.get("chunks") or []
        if not isinstance(chunks, list) or not all(
            isinstance(item, dict) for item in chunks
        ):
            raise RAGFlowProtocolError("RAGFlow retrieval chunks are invalid")
        total = result.get("total", len(chunks))
        if not isinstance(total, int) or total < 0:
            raise RAGFlowProtocolError("RAGFlow retrieval total is invalid")
        return RAGFlowRetrievalResult(chunks=chunks, total=total)


async def ingest_into_ragflow(
    *,
    settings: Settings,
    target_dataset: str,
    title: str | None,
    canonical_url: str | None,
    source_artifact_refs: list[dict[str, Any]],
    metadata_json: dict[str, Any],
    source_document_version_id: str,
) -> RAGFlowIngestionResult:
    raise RuntimeError("bare RAGFlow ingestion disabled; use durable provider receipts")


async def check_ragflow_config(settings: Settings) -> RAGFlowConfigCheck:
    issues: list[str] = []
    details: dict[str, Any] = {
        "api_base_configured": bool(settings.ragflow_api_base),
        "api_key_configured": bool(settings.ragflow_api_key),
    }
    if not settings.ragflow_enabled:
        issues.append("RAGFlow API base or API key is not configured")
        return RAGFlowConfigCheck(
            enabled=False,
            reachable=False,
            has_default_embedding=False,
            issues=issues,
            details=details,
        )

    client = RAGFlowClient(settings)
    try:
        datasets = await client.list_datasets(page_size=1)
        tenant = await client.get_tenant_models()
    except Exception as exc:
        issues.append(str(exc))
        return RAGFlowConfigCheck(
            enabled=True,
            reachable=False,
            has_default_embedding=False,
            issues=issues,
            details=details,
        )
    finally:
        await client.close()

    embd_id = str(tenant.get("embd_id") or "")
    tenant_embd_id = tenant.get("tenant_embd_id")
    has_default_embedding = bool(embd_id or tenant_embd_id)
    if not has_default_embedding:
        issues.append("RAGFlow tenant has no default embedding model")

    details.update(
        {
            "dataset_list_accessible": True,
            "visible_dataset_count_sample": len(datasets),
            "tenant_id": tenant.get("tenant_id"),
            "tenant_name": tenant.get("name"),
            "embd_id": embd_id,
            "tenant_embd_id": tenant_embd_id,
            "llm_id": tenant.get("llm_id"),
        }
    )
    return RAGFlowConfigCheck(
        enabled=True,
        reachable=True,
        has_default_embedding=has_default_embedding,
        issues=issues,
        details=details,
    )


async def _wait_for_document_parse(
    *,
    client: RAGFlowClient,
    dataset_id: str,
    document_id: str,
    timeout_seconds: int,
    interval_seconds: float,
) -> dict[str, Any]:
    """Legacy helper only; durable ingestion uses persisted single-query steps."""
    deadline = time.monotonic() + timeout_seconds
    last_doc: dict[str, Any] = {}
    while time.monotonic() <= deadline:
        last_doc = await client.get_document(dataset_id, document_id)
        run = _normalise_run(last_doc.get("run"))
        if run == "DONE":
            return last_doc
        if run == "FAIL":
            raise RAGFlowParseError(
                str(last_doc.get("progress_msg") or "RAGFlow parse failed")
            )
        if run == "CANCEL":
            raise RAGFlowParseCancelledError(
                str(last_doc.get("progress_msg") or "RAGFlow parse was cancelled")
            )
        await _sleep(interval_seconds)
    raise RAGFlowParseError(
        "RAGFlow parse timed out for document "
        f"{document_id}: {last_doc.get('progress_msg')}"
    )


# RAGFlow 的 run 在库里是数字（TaskStatus，见其 common/constants.py），
# HTTP 列表端点经 map_doc_keys() 映射为文本再返回。两种都接受，避免
# RAGFlow 版本漂移导致判定静默失效——只认文本时，若某版本直接吐数字，
# 所有取值都落不进终态，只会一路轮询到超时。
_RUN_ALIASES = {
    "0": "UNSTART",
    "1": "RUNNING",
    "2": "CANCEL",
    "3": "DONE",
    "4": "FAIL",
    "5": "SCHEDULE",
}


def _normalise_run(value: object) -> str:
    run = str(value if value is not None else "").strip().upper()
    return _RUN_ALIASES.get(run, run)


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


def _maybe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _maybe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
