"""The adapter boundary is executable, not just a Protocol declaration."""

from __future__ import annotations

import ast
import hashlib
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from app.application.ports.knowledge_provider import (
    ParseStatus,
    ProviderDefinition,
    ProviderProtocolError,
    ProviderTimeoutError,
)
from app.application.services import knowledge_retrieval_service as retrieval
from app.infrastructure.external import knowledge_provider as composition
from app.infrastructure.external.ragflow import RAGFlowClient, RAGFlowRetrievalResult
from app.infrastructure.external.ragflow_provider import (
    RAGFlowProvider,
    _document,
    normalize_retrieval,
)
from core.config import Settings


def settings():
    return Settings(
        RAGFLOW_API_BASE="https://provider.example.test/", RAGFLOW_API_KEY="test-only"
    )


@pytest.mark.parametrize(
    "value,expected",
    [
        ("0", ParseStatus.NOT_STARTED),
        (1, ParseStatus.RUNNING),
        (2, ParseStatus.CANCELLED),
        ("3", ParseStatus.SUCCEEDED),
        (4, ParseStatus.FAILED),
        (5, ParseStatus.QUEUED),
        (" done ", ParseStatus.SUCCEEDED),
        ("new-state", ParseStatus.UNKNOWN),
        (None, ParseStatus.UNKNOWN),
    ],
)
def test_adapter_normalizes_vendor_parse_values(value, expected):
    document = _document({"id": "d", "dataset_id": "s", "name": "n", "run": value})
    assert document.parse_status is expected


@pytest.mark.parametrize("field", ["id", "dataset_id", "name"])
@pytest.mark.parametrize("bad", [None, "", 123])
def test_adapter_rejects_invalid_document_identity(field, bad):
    raw = {"id": "d", "dataset_id": "s", "name": "n"}
    raw[field] = bad
    with pytest.raises(ProviderProtocolError):
        _document(raw)


async def test_scope_digest_is_compatible_and_tenant_changes_are_visible():
    client = AsyncMock()
    client.get_tenant_models.return_value = {"tenant_id": "one"}
    adapter = RAGFlowProvider(settings(), client=client)
    assert (
        await adapter.scope()
        == hashlib.sha256(b"https://provider.example.test|one").hexdigest()
    )
    old = await adapter.scope()
    client.get_tenant_models.return_value = {"tenant_id": "two"}
    assert await adapter.scope() != old
    client.get_tenant_models.return_value = {}
    with pytest.raises(ProviderProtocolError):
        await adapter.scope()
    await adapter.close()
    client.close.assert_awaited_once()


def test_adapter_normalizes_retrieval_aliases_and_invalid_scores():
    result = normalize_retrieval(
        RAGFlowRetrievalResult(
            chunks=[
                {
                    "chunk_id": "c",
                    "doc_id": "d",
                    "dataset_id": "s",
                    "content_with_weight": " text ",
                    "score": "0.7",
                    "vector_similarity": float("nan"),
                    "term_similarity": 9,
                }
            ],
            total=3,
        )
    )
    chunk = result.chunks[0]
    assert (chunk.id, chunk.dataset_id, chunk.document_id, chunk.content) == (
        "c",
        "s",
        "d",
        "text",
    )
    assert (chunk.score, chunk.term_similarity, chunk.vector_similarity) == (0.7, 1, 0)
    assert result.total == 3


async def test_real_http_adapter_preserves_retrieval_filter_and_neutral_result():
    requests = []

    def handler(request):
        import json

        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "chunks": [
                        {
                            "id": "c",
                            "doc_id": "d",
                            "dataset_id": "s",
                            "content_with_weight": "hit",
                            "similarity": 0.9,
                        }
                    ],
                    "total": 1,
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = RAGFlowClient(settings(), client=http)
        adapter = RAGFlowProvider(settings(), client=client)
        result = await adapter.retrieve(
            question="q", dataset_ids=["s"], document_ids=["d"], top_k=2
        )
        assert result.chunks[0].content == "hit"
        assert requests[0]["dataset_ids"] == ["s"]
        assert requests[0]["document_ids"] == ["d"]
        await adapter.close()
        assert (
            not http.is_closed
        )  # Injected HTTP client's lifetime stays with its owner.


async def test_transport_timeout_is_a_neutral_provider_error():
    def handler(request):
        raise httpx.ReadTimeout("test", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        adapter = RAGFlowProvider(
            settings(), client=RAGFlowClient(settings(), client=http)
        )
        with pytest.raises(ProviderTimeoutError):
            await adapter.retrieve(
                question="q", dataset_ids=["s"], document_ids=["d"], top_k=1
            )


def test_composition_is_explicit_and_unknown_provider_fails_closed(monkeypatch):
    definition = composition.provider_definition(settings())
    assert definition.name == "ragflow" and definition.enabled
    assert not composition.provider_definition(Settings()).enabled
    monkeypatch.setattr(
        composition,
        "provider_definition",
        lambda _: replace(definition, name="uninstalled"),
    )
    with pytest.raises(ProviderProtocolError, match="unsupported"):
        composition.create_provider(settings())


def test_business_modules_do_not_import_vendor_client_or_wire_helpers():
    root = Path(__file__).resolve().parents[1]
    for name in (
        "provider_delivery",
        "knowledge_retrieval_service",
        "ingestion_authorization",
    ):
        source = (root / f"app/application/services/{name}.py").read_text()
        tree = ast.parse(source)
        imports = [
            node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        ]
        assert "app.infrastructure.external.ragflow" not in imports
        assert "app.infrastructure.external.ragflow_provider" not in imports
        for symbol in (
            "RAGFlowClient",
            "_normalise_run",
            "content_with_weight",
            "get_tenant_models",
            "ragflow_api_base",
        ):
            assert symbol not in source
    source = (
        root / "app/application/services/knowledge_ingestion_service.py"
    ).read_text()
    tree = ast.parse(source)
    vendor_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "app.infrastructure.external.ragflow"
    ]
    # Explicitly retained vendor-specific diagnostic endpoint, not the data plane.
    assert [[alias.name for alias in node.names] for node in vendor_imports] == [
        ["check_ragflow_config"]
    ]
    assert "RAGFlowClient" not in source and "settings.ragflow_" not in source
    port = (root / "app/application/ports/knowledge_provider.py").read_text()
    assert "from app.infrastructure" not in port


async def test_v1_rejects_another_provider_before_lookup_or_network(monkeypatch):
    from test_knowledge_retrieval import _principal, _request

    from app.application.errors.exceptions import ServiceUnavailableError

    config = Settings(RETRIEVAL_DATASET_ALLOWLIST="market-news")
    monkeypatch.setattr(retrieval, "get_settings", lambda: config)
    monkeypatch.setattr(
        retrieval,
        "provider_definition",
        lambda _: ProviderDefinition("test-index", True, "hash", 120, 1),
    )
    session = AsyncMock()
    principal = _principal(config.retrieval_auth_required_scope)
    with pytest.raises(ServiceUnavailableError, match="does not support"):
        await retrieval.retrieve_knowledge(
            session, _request(), service_principal=principal
        )
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("failure", ["timeout", "protocol", "unavailable", "cancel"])
async def test_retrieval_closes_port_and_preserves_error_boundary(monkeypatch, failure):
    import asyncio

    from test_knowledge_retrieval import _principal, _request, _version

    from app.application.errors.exceptions import (
        BadGatewayError,
        GatewayTimeoutError,
        ServiceUnavailableError,
    )
    from app.application.ports.knowledge_provider import ProviderError

    config = settings().model_copy(
        update={"retrieval_dataset_allowlist": "market-news"}
    )
    monkeypatch.setattr(retrieval, "get_settings", lambda: config)
    monkeypatch.setattr(
        retrieval, "_eligible_versions", AsyncMock(return_value=[_version()])
    )
    errors = {
        "timeout": (ProviderTimeoutError(), GatewayTimeoutError),
        "protocol": (ProviderProtocolError(), BadGatewayError),
        "unavailable": (ProviderError(), ServiceUnavailableError),
        "cancel": (asyncio.CancelledError(), asyncio.CancelledError),
    }
    raised, expected = errors[failure]
    client = AsyncMock()
    client.retrieve.side_effect = raised
    monkeypatch.setattr(retrieval, "create_provider", lambda *args, **kwargs: client)
    with pytest.raises(expected):
        await retrieval.retrieve_knowledge(
            AsyncMock(), _request(), service_principal=_principal()
        )
    client.close.assert_awaited_once()
