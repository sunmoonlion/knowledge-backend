from __future__ import annotations

import base64
import hashlib
import hmac
import mimetypes
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from core.config import Settings


class RAGFlowError(RuntimeError):
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


class RAGFlowClient:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None) -> None:
        if not settings.ragflow_api_base or not settings.ragflow_api_key:
            raise RAGFlowError("RAGFlow is not configured")
        base = settings.ragflow_api_base.rstrip("/")
        if not base.endswith("/api/v1"):
            base = f"{base}/api/v1"
        self._base = base
        self._api_key = settings.ragflow_api_key
        self._client = client or httpx.AsyncClient(timeout=30)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._api_key}"}

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = await self._client.request(
            method, f"{self._base}{path}", headers=self._headers(), **kwargs
        )
        response.raise_for_status()
        data = response.json()
        if data.get("code") != 0:
            raise RAGFlowError(str(data.get("message") or data))
        return data

    async def ensure_dataset(self, name: str) -> dict[str, Any]:
        list_data = await self._request("GET", "/datasets", params={"name": name, "page_size": 30})
        for item in list_data.get("data") or []:
            if item.get("name") == name:
                return item
        create_data = await self._request(
            "POST",
            "/datasets",
            json={"name": name, "chunk_method": "naive", "permission": "me"},
        )
        return create_data["data"]

    async def upload_document(self, dataset_id: str, artifact: ArtifactContent) -> dict[str, Any]:
        files = {
            "file": (
                artifact.filename,
                artifact.content,
                artifact.content_type or "text/plain",
            )
        }
        data = await self._request("POST", f"/datasets/{dataset_id}/documents", files=files)
        documents = data.get("data") or []
        if not documents:
            raise RAGFlowError("RAGFlow upload returned no document")
        return documents[0]

    async def parse_document(self, dataset_id: str, document_id: str) -> None:
        await self._request(
            "POST", f"/datasets/{dataset_id}/documents/parse", json={"document_ids": [document_id]}
        )

    async def get_document(self, dataset_id: str, document_id: str) -> dict[str, Any]:
        data = await self._request("GET", f"/datasets/{dataset_id}/documents/{document_id}")
        return data["data"]


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
            document_name=str(final_doc.get("name") or document.get("name") or artifact.filename),
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
                raise RAGFlowError(str(last_doc.get("progress_msg") or "RAGFlow parse failed"))
            return last_doc
        await _sleep(interval_seconds)
    raise RAGFlowError(
        f"RAGFlow parse timed out for document {document_id}: {last_doc.get('progress_msg')}"
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
    inline = _inline_text(metadata_json)
    if inline is not None:
        return ArtifactContent(
            filename=_filename(title, canonical_url, source_document_version_id),
            content=inline.encode("utf-8"),
            content_type="text/plain; charset=utf-8",
        )

    for ref in source_artifact_refs:
        content = await _resolve_artifact_ref(settings, ref)
        if content is not None:
            return content

    raise RAGFlowError("No readable artifact content found for ingestion job")


async def _resolve_artifact_ref(settings: Settings, ref: dict[str, Any]) -> ArtifactContent | None:
    uri = ref.get("uri")
    if isinstance(uri, str) and uri.startswith(("http://", "https://")):
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(uri)
            response.raise_for_status()
            return ArtifactContent(
                filename=_name_from_ref(ref),
                content=response.content,
                content_type=str(response.headers.get("content-type") or ref.get("content_type") or "text/plain"),
            )
    if isinstance(uri, str) and uri.startswith("data:"):
        return _decode_data_uri(uri, ref)
    if isinstance(uri, str) and uri.startswith("s3://"):
        parts = urlsplit(uri)
        return await _fetch_s3_object(settings, parts.netloc, parts.path.lstrip("/"), ref)
    bucket = ref.get("bucket")
    object_key = ref.get("object_key")
    if isinstance(bucket, str) and isinstance(object_key, str):
        return await _fetch_s3_object(settings, bucket, object_key, ref)
    return None


async def _fetch_s3_object(
    settings: Settings, bucket: str, object_key: str, ref: dict[str, Any]
) -> ArtifactContent:
    if not settings.s3_endpoint or not settings.s3_access_key_id or not settings.s3_secret_access_key:
        raise RAGFlowError("S3 artifact provided but S3 credentials are not configured")
    endpoint = settings.s3_endpoint.rstrip("/")
    parsed = urlsplit(endpoint)
    if not parsed.scheme or not parsed.netloc:
        raise RAGFlowError("S3_ENDPOINT must include scheme and host")
    if settings.s3_force_path_style:
        path = "/" + "/".join(quote(part, safe="") for part in [bucket, *object_key.split("/")])
        url = f"{parsed.scheme}://{parsed.netloc}{path}"
        host = parsed.netloc
    else:
        path = "/" + "/".join(quote(part, safe="") for part in object_key.split("/"))
        host = f"{bucket}.{parsed.netloc}"
        url = f"{parsed.scheme}://{host}{path}"
    headers = _s3_sigv4_headers(
        method="GET",
        host=host,
        canonical_uri=path,
        region=settings.s3_region,
        access_key=settings.s3_access_key_id,
        secret_key=settings.s3_secret_access_key,
    )
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return ArtifactContent(
            filename=_name_from_ref(ref, object_key=object_key),
            content=response.content,
            content_type=str(response.headers.get("content-type") or ref.get("content_type") or _guess_type(object_key)),
        )


def _s3_sigv4_headers(
    *,
    method: str,
    host: str,
    canonical_uri: str,
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
    canonical_headers = "".join(f"{key}:{headers[key]}\n" for key in signed_headers.split(";"))
    canonical_request = "\n".join(
        [method, canonical_uri, "", canonical_headers, signed_headers, payload_hash]
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
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return headers


def _aws_signing_key(secret_key: str, datestamp: str, region: str) -> bytes:
    date_key = hmac.new(f"AWS4{secret_key}".encode(), datestamp.encode(), hashlib.sha256).digest()
    region_key = hmac.new(date_key, region.encode(), hashlib.sha256).digest()
    service_key = hmac.new(region_key, b"s3", hashlib.sha256).digest()
    return hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()


def _decode_data_uri(uri: str, ref: dict[str, Any]) -> ArtifactContent:
    header, _, payload = uri.partition(",")
    content_type = header[5:].split(";")[0] or str(ref.get("content_type") or "text/plain")
    if ";base64" in header:
        content = base64.b64decode(payload)
    else:
        content = payload.encode("utf-8")
    return ArtifactContent(filename=_name_from_ref(ref), content=content, content_type=content_type)


def _inline_text(metadata_json: dict[str, Any]) -> str | None:
    for key in ("text", "content", "markdown"):
        value = metadata_json.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _dataset_name(target_dataset: str) -> str:
    return target_dataset.strip() or "default"


def _filename(title: str | None, canonical_url: str | None, source_document_version_id: str) -> str:
    stem = title or _basename_from_url(canonical_url) or source_document_version_id
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-._") or source_document_version_id
    if "." not in stem:
        stem = f"{stem}.txt"
    return stem[:180]


def _basename_from_url(url: str | None) -> str | None:
    if not url:
        return None
    path = PurePosixPath(urlsplit(url).path)
    return path.name or None


def _name_from_ref(ref: dict[str, Any], object_key: str | None = None) -> str:
    for value in (ref.get("filename"), ref.get("name"), object_key, ref.get("object_key")):
        if isinstance(value, str) and value.strip():
            return PurePosixPath(value).name or "document.txt"
    artifact_type = ref.get("artifact_type") or ref.get("kind") or "document"
    return f"{artifact_type}.txt"


def _guess_type(name: str) -> str:
    return mimetypes.guess_type(name)[0] or "text/plain"


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
