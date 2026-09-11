"""Provider response shapes checked against the deployed v0.25.4 source."""

import hashlib

import httpx
import pytest

from app.infrastructure.external.ragflow import RAGFlowClient, RAGFlowProtocolError
from core.config import Settings


def provider(client):
    return RAGFlowClient(
        Settings(
            RAGFLOW_API_BASE="https://ragflow.example.test", RAGFLOW_API_KEY="test-only"
        ),
        client=client,
    )


async def test_dataset_search_paginates_without_the_missing_name_auth_error():
    calls = []

    def handle(request):
        assert "name" not in request.url.params
        page = int(request.url.params["page"])
        calls.append(page)
        return httpx.Response(
            200,
            json={
                "code": 0,
                "total_datasets": 2,
                "data": [{"id": str(page), "name": "wanted" if page == 2 else "other"}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert await provider(client).find_datasets("wanted") == [
            {"id": "2", "name": "wanted"}
        ]
    assert calls == [1, 2]


async def test_dataset_lookup_refuses_an_incomplete_page():
    def handle(request):
        return httpx.Response(200, json={"code": 0, "total_datasets": 3, "data": []})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(RAGFlowProtocolError, match="truncated"):
            await provider(client).find_datasets("wanted")


async def test_document_lookup_filters_exact_stable_filename():
    def handle(request):
        assert request.url.params["keywords"] == "stable.txt"
        assert "name" not in request.url.params
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "total": 2,
                    "docs": [
                        {"id": "wrong", "name": "stable.txt (1)"},
                        {"id": "right", "name": "stable.txt"},
                    ],
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert await provider(client).find_documents("dataset", "stable.txt") == [
            {"id": "right", "name": "stable.txt"}
        ]


@pytest.mark.parametrize("content", [b"other", b"too-long-content"])
async def test_document_download_does_not_accept_wrong_bytes(content):
    def handle(request):
        return httpx.Response(200, content=content)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(RAGFlowProtocolError):
            await provider(client).verify_document_content(
                "dataset",
                "document",
                size=5,
                sha256=hashlib.sha256(b"hello").hexdigest(),
            )
