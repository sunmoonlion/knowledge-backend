"""Immutable source Artifact resolution, independent of any index provider."""

from __future__ import annotations

import hashlib
import hmac
import re
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote, unquote, urlsplit

import httpx

from app.application.ports.knowledge_provider import ArtifactContent, ArtifactError
from core.config import Settings


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
        raise ArtifactError(
            "Artifact contract requires exactly one immutable S3 artifact"
        )
    return await _resolve_artifact_ref(settings, source_artifact_refs[0])


async def _resolve_artifact_ref(
    settings: Settings, ref: dict[str, Any]
) -> ArtifactContent:
    uri = ref.get("uri")
    if not isinstance(uri, str) or not uri.startswith("s3://"):
        raise ArtifactError("Artifact contract only accepts s3:// references")
    parts = urlsplit(uri)
    if parts.query or parts.fragment or parts.username or parts.password:
        raise ArtifactError(
            "Artifact S3 URI must not contain query, fragment or userinfo"
        )
    bucket = parts.netloc
    object_key = unquote(parts.path.lstrip("/"))
    if bucket not in settings.artifact_bucket_allowlist:
        raise ArtifactError(f"Artifact bucket is not allowed: {bucket}")
    if not object_key or any(part in {"", ".", ".."} for part in object_key.split("/")):
        raise ArtifactError("Artifact object key contains an invalid path segment")
    prefixes = settings.artifact_prefix_allowlist
    if not prefixes or not any(object_key.startswith(prefix) for prefix in prefixes):
        raise ArtifactError("Artifact object key is outside the allowed prefixes")
    return await _fetch_s3_object(settings, bucket, object_key, ref)


async def _fetch_s3_object(
    settings: Settings, bucket: str, object_key: str, ref: dict[str, Any]
) -> ArtifactContent:
    if (
        not settings.s3_endpoint
        or not settings.s3_access_key_id
        or not settings.s3_secret_access_key
    ):
        raise ArtifactError(
            "S3 artifact provided but S3 credentials are not configured"
        )
    endpoint = settings.s3_endpoint.rstrip("/")
    parsed = urlsplit(endpoint)
    if not parsed.scheme or not parsed.netloc:
        raise ArtifactError("S3_ENDPOINT must include scheme and host")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ArtifactError("S3_ENDPOINT must not contain path, query or fragment")

    storage_version = str(ref.get("storage_version") or "")
    expected_sha256 = str(ref.get("sha256") or "")
    expected_content_type = str(ref.get("content_type") or "").lower()
    size_value = ref.get("size_bytes")
    if not isinstance(size_value, int) or isinstance(size_value, bool):
        raise ArtifactError("Artifact size_bytes is invalid")
    expected_size = size_value
    if not storage_version:
        raise ArtifactError("Artifact storage version is required")
    if not re.fullmatch(r"[a-f0-9]{64}", expected_sha256):
        raise ArtifactError("Artifact sha256 is invalid")
    if expected_size < 1 or expected_size > settings.artifact_max_size_bytes:
        raise ArtifactError("Artifact size exceeds the configured maximum")
    media_type = _media_type(expected_content_type)
    if media_type not in settings.artifact_content_type_allowlist:
        raise ArtifactError(f"Artifact content type is not allowed: {media_type}")

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
                        raise ArtifactError(
                            "S3 object exceeded the declared artifact size"
                        )
                    chunks.append(chunk)
        except httpx.HTTPError as exc:
            raise _s3_http_error(exc) from exc
    content = b"".join(chunks)
    if len(content) != expected_size:
        raise ArtifactError(
            f"S3 object size mismatch: expected {expected_size}, got {len(content)}"
        )
    actual_sha256 = hashlib.sha256(content).hexdigest()
    if not hmac.compare_digest(actual_sha256, expected_sha256):
        raise ArtifactError("S3 object sha256 mismatch")
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
        raise ArtifactError("S3 object storage version mismatch")
    content_length = response.headers.get("content-length")
    try:
        actual_size = int(content_length) if content_length is not None else None
    except ValueError as exc:
        raise ArtifactError("S3 object Content-Length is invalid") from exc
    if actual_size != expected_size:
        raise ArtifactError("S3 object Content-Length does not match artifact contract")
    response_content_type = _media_type(response.headers.get("content-type") or "")
    if response_content_type != expected_content_type:
        raise ArtifactError("S3 object content type does not match artifact contract")


def _media_type(value: str) -> str:
    return value.split(";", 1)[0].strip().lower()


def _s3_http_error(exc: httpx.HTTPError) -> ArtifactError:
    if isinstance(exc, httpx.HTTPStatusError):
        return ArtifactError(
            f"S3 object request failed with HTTP {exc.response.status_code}"
        )
    return ArtifactError("S3 object request failed")


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
