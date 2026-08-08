from __future__ import annotations

import hashlib
import hmac
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote, unquote, urlsplit

import httpx

from core.config import Settings


class RAGFlowError(RuntimeError):
    pass


class RAGFlowTimeoutError(RAGFlowError):
    pass


class RAGFlowProtocolError(RAGFlowError):
    pass


@dataclass(frozen=True)
class ArtifactContent:
    filename: str
    content: bytes
    content_type: str


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

    async def ensure_dataset(self, name: str) -> dict[str, Any]:
        list_data = await self._request("GET", "/datasets", params={"page_size": 100})
        for item in list_data.get("data") or []:
            if item.get("name") == name:
                return item
        create_data = await self._request(
            "POST",
            "/datasets",
            json={"name": name, "chunk_method": "naive", "permission": "me"},
        )
        return create_data["data"]

    async def list_datasets(self, page_size: int = 10) -> list[dict[str, Any]]:
        data = await self._request("GET", "/datasets", params={"page_size": page_size})
        datasets = data.get("data") or []
        return [item for item in datasets if isinstance(item, dict)]

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
    artifact = await resolve_artifact_content(
        settings=settings,
        source_artifact_refs=source_artifact_refs,
        title=title,
        canonical_url=canonical_url,
        metadata_json=metadata_json,
        source_document_version_id=source_document_version_id,
    )
    client = RAGFlowClient(settings)
    try:
        dataset = await client.ensure_dataset(_dataset_name(target_dataset))
        document = await client.upload_document(dataset["id"], artifact)
        document_id = document["id"]
        await client.parse_document(dataset["id"], document_id)
        final_doc = await _wait_for_document_parse(
            client=client,
            dataset_id=dataset["id"],
            document_id=document_id,
            timeout_seconds=settings.ragflow_parse_timeout_seconds,
            interval_seconds=settings.ragflow_parse_poll_interval_seconds,
        )
        return RAGFlowIngestionResult(
            dataset_id=dataset["id"],
            dataset_name=dataset["name"],
            document_id=document_id,
            document_name=str(
                final_doc.get("name") or document.get("name") or artifact.filename
            ),
            parse_status=str(final_doc.get("run") or ""),
            chunk_count=_maybe_int(final_doc.get("chunk_count")),
            token_count=_maybe_int(final_doc.get("token_count")),
            metadata={
                "ragflow_dataset_id": dataset["id"],
                "ragflow_dataset_name": dataset["name"],
                "ragflow_document_name": str(
                    final_doc.get("name") or document.get("name") or artifact.filename
                ),
                "ragflow_parse_status": str(final_doc.get("run") or ""),
                "ragflow_chunk_count": _maybe_int(final_doc.get("chunk_count")),
                "ragflow_token_count": _maybe_int(final_doc.get("token_count")),
            },
        )
    finally:
        await client.close()


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
    deadline = time.monotonic() + timeout_seconds
    last_doc: dict[str, Any] = {}
    terminal = {"DONE", "FAIL", "CANCEL"}
    while time.monotonic() <= deadline:
        last_doc = await client.get_document(dataset_id, document_id)
        run = str(last_doc.get("run") or "").upper()
        progress = _maybe_float(last_doc.get("progress"))
        if run in terminal or (progress is not None and progress >= 1.0):
            if run == "FAIL":
                raise RAGFlowError(
                    str(last_doc.get("progress_msg") or "RAGFlow parse failed")
                )
            return last_doc
        await _sleep(interval_seconds)
    raise RAGFlowError(
        "RAGFlow parse timed out for document "
        f"{document_id}: {last_doc.get('progress_msg')}"
    )


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


async def resolve_artifact_content(
    *,
    settings: Settings,
    source_artifact_refs: list[dict[str, Any]],
    title: str | None,
    canonical_url: str | None,
    metadata_json: dict[str, Any],
    source_document_version_id: str,
) -> ArtifactContent:
    del title, canonical_url, metadata_json, source_document_version_id
    if len(source_artifact_refs) != 1:
        raise RAGFlowError(
            "Artifact contract requires exactly one immutable S3 artifact"
        )
    return await _resolve_artifact_ref(settings, source_artifact_refs[0])


async def _resolve_artifact_ref(
    settings: Settings, ref: dict[str, Any]
) -> ArtifactContent:
    uri = ref.get("uri")
    if not isinstance(uri, str) or not uri.startswith("s3://"):
        raise RAGFlowError("Artifact contract only accepts s3:// references")
    parts = urlsplit(uri)
    if parts.query or parts.fragment or parts.username or parts.password:
        raise RAGFlowError(
            "Artifact S3 URI must not contain query, fragment or userinfo"
        )
    bucket = parts.netloc
    object_key = unquote(parts.path.lstrip("/"))
    if bucket not in settings.artifact_bucket_allowlist:
        raise RAGFlowError(f"Artifact bucket is not allowed: {bucket}")
    if not object_key or any(part in {"", ".", ".."} for part in object_key.split("/")):
        raise RAGFlowError("Artifact object key contains an invalid path segment")
    prefixes = settings.artifact_prefix_allowlist
    if not prefixes or not any(object_key.startswith(prefix) for prefix in prefixes):
        raise RAGFlowError("Artifact object key is outside the allowed prefixes")
    return await _fetch_s3_object(settings, bucket, object_key, ref)


async def _fetch_s3_object(
    settings: Settings, bucket: str, object_key: str, ref: dict[str, Any]
) -> ArtifactContent:
    if (
        not settings.s3_endpoint
        or not settings.s3_access_key_id
        or not settings.s3_secret_access_key
    ):
        raise RAGFlowError("S3 artifact provided but S3 credentials are not configured")
    endpoint = settings.s3_endpoint.rstrip("/")
    parsed = urlsplit(endpoint)
    if not parsed.scheme or not parsed.netloc:
        raise RAGFlowError("S3_ENDPOINT must include scheme and host")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise RAGFlowError("S3_ENDPOINT must not contain path, query or fragment")

    storage_version = str(ref.get("storage_version") or "")
    expected_sha256 = str(ref.get("sha256") or "")
    expected_content_type = str(ref.get("content_type") or "").lower()
    size_value = ref.get("size_bytes")
    if not isinstance(size_value, int) or isinstance(size_value, bool):
        raise RAGFlowError("Artifact size_bytes is invalid")
    expected_size = size_value
    if not storage_version:
        raise RAGFlowError("Artifact storage version is required")
    if not re.fullmatch(r"[a-f0-9]{64}", expected_sha256):
        raise RAGFlowError("Artifact sha256 is invalid")
    if expected_size < 1 or expected_size > settings.artifact_max_size_bytes:
        raise RAGFlowError("Artifact size exceeds the configured maximum")
    media_type = _media_type(expected_content_type)
    if media_type not in settings.artifact_content_type_allowlist:
        raise RAGFlowError(f"Artifact content type is not allowed: {media_type}")

    if settings.s3_force_path_style:
        path = "/" + "/".join(
            quote(part, safe="") for part in [bucket, *object_key.split("/")]
        )
        host = parsed.netloc
    else:
        path = "/" + "/".join(quote(part, safe="") for part in object_key.split("/"))
        host = f"{bucket}.{parsed.netloc}"
    canonical_query = f"versionId={quote(storage_version, safe='-_.~')}"
    url = f"{parsed.scheme}://{host}{path}?{canonical_query}"
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            head = await client.head(
                url,
                headers=_s3_sigv4_headers(
                    method="HEAD",
                    host=host,
                    canonical_uri=path,
                    canonical_query=canonical_query,
                    region=settings.s3_region,
                    access_key=settings.s3_access_key_id,
                    secret_key=settings.s3_secret_access_key,
                ),
            )
            head.raise_for_status()
        except httpx.HTTPError as exc:
            raise _s3_http_error(exc) from exc
        _verify_s3_headers(
            head,
            storage_version=storage_version,
            expected_size=expected_size,
            expected_content_type=media_type,
        )
        chunks: list[bytes] = []
        received = 0
        try:
            async with client.stream(
                "GET",
                url,
                headers=_s3_sigv4_headers(
                    method="GET",
                    host=host,
                    canonical_uri=path,
                    canonical_query=canonical_query,
                    region=settings.s3_region,
                    access_key=settings.s3_access_key_id,
                    secret_key=settings.s3_secret_access_key,
                ),
            ) as response:
                response.raise_for_status()
                _verify_s3_headers(
                    response,
                    storage_version=storage_version,
                    expected_size=expected_size,
                    expected_content_type=media_type,
                )
                async for chunk in response.aiter_bytes():
                    received += len(chunk)
                    if (
                        received > expected_size
                        or received > settings.artifact_max_size_bytes
                    ):
                        raise RAGFlowError(
                            "S3 object exceeded the declared artifact size"
                        )
                    chunks.append(chunk)
        except httpx.HTTPError as exc:
            raise _s3_http_error(exc) from exc
    content = b"".join(chunks)
    if len(content) != expected_size:
        raise RAGFlowError(
            f"S3 object size mismatch: expected {expected_size}, got {len(content)}"
        )
    actual_sha256 = hashlib.sha256(content).hexdigest()
    if not hmac.compare_digest(actual_sha256, expected_sha256):
        raise RAGFlowError("S3 object sha256 mismatch")
    return ArtifactContent(
        filename=_name_from_ref(ref, object_key=object_key),
        content=content,
        content_type=expected_content_type,
    )


def _verify_s3_headers(
    response: httpx.Response,
    *,
    storage_version: str,
    expected_size: int,
    expected_content_type: str,
) -> None:
    response_version = response.headers.get("x-amz-version-id")
    if response_version != storage_version:
        raise RAGFlowError("S3 object storage version mismatch")
    content_length = response.headers.get("content-length")
    try:
        actual_size = int(content_length) if content_length is not None else None
    except ValueError as exc:
        raise RAGFlowError("S3 object Content-Length is invalid") from exc
    if actual_size != expected_size:
        raise RAGFlowError("S3 object Content-Length does not match artifact contract")
    response_content_type = _media_type(response.headers.get("content-type") or "")
    if response_content_type != expected_content_type:
        raise RAGFlowError("S3 object content type does not match artifact contract")


def _media_type(value: str) -> str:
    return value.split(";", 1)[0].strip().lower()


def _s3_http_error(exc: httpx.HTTPError) -> RAGFlowError:
    if isinstance(exc, httpx.HTTPStatusError):
        return RAGFlowError(
            f"S3 object request failed with HTTP {exc.response.status_code}"
        )
    return RAGFlowError("S3 object request failed")


def _s3_sigv4_headers(
    *,
    method: str,
    host: str,
    canonical_uri: str,
    canonical_query: str = "",
    region: str,
    access_key: str,
    secret_key: str,
) -> dict[str, str]:
    now = datetime.now(UTC)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()
    headers = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_headers = "".join(
        f"{key}:{headers[key]}\n" for key in signed_headers.split(";")
    )
    canonical_request = "\n".join(
        [
            method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    scope = f"{datestamp}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    signing_key = _aws_signing_key(secret_key, datestamp, region)
    signature = hmac.new(
        signing_key, string_to_sign.encode(), hashlib.sha256
    ).hexdigest()
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return headers


def _aws_signing_key(secret_key: str, datestamp: str, region: str) -> bytes:
    date_key = hmac.new(
        f"AWS4{secret_key}".encode(), datestamp.encode(), hashlib.sha256
    ).digest()
    region_key = hmac.new(date_key, region.encode(), hashlib.sha256).digest()
    service_key = hmac.new(region_key, b"s3", hashlib.sha256).digest()
    return hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()


def _dataset_name(target_dataset: str) -> str:
    return target_dataset.strip() or "default"


def _name_from_ref(ref: dict[str, Any], object_key: str | None = None) -> str:
    for value in (
        ref.get("filename"),
        ref.get("name"),
        object_key,
        ref.get("object_key"),
    ):
        if isinstance(value, str) and value.strip():
            return PurePosixPath(value).name or "document.txt"
    artifact_type = ref.get("artifact_type") or ref.get("kind") or "document"
    return f"{artifact_type}.txt"


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
