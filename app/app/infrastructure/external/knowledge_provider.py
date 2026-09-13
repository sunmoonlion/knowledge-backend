"""The single composition point for Knowledge provider configuration.

Only RAGFlow is enabled in this release. A new implementation also requires
explicit binding/journal migration and a retrieval-v1 contract decision.
"""

import hashlib

from app.application.ports.knowledge_provider import (
    KnowledgeProvider,
    ProviderDefinition,
    ProviderProtocolError,
)
from app.infrastructure.external.ragflow_provider import RAGFlowProvider
from core.config import Settings


def provider_definition(settings: Settings) -> ProviderDefinition:
    return ProviderDefinition(
        name="ragflow",
        enabled=settings.ragflow_enabled,
        endpoint_fingerprint=hashlib.sha256(
            (settings.ragflow_api_base or "").rstrip("/").encode()
        ).hexdigest(),
        parse_timeout_seconds=settings.ragflow_parse_timeout_seconds,
        parse_poll_interval_seconds=settings.ragflow_parse_poll_interval_seconds,
    )


def create_provider(
    settings: Settings, *, timeout_seconds: float = 30
) -> KnowledgeProvider:
    definition = provider_definition(settings)
    if definition.name != "ragflow":
        raise ProviderProtocolError("knowledge provider implementation is unsupported")
    return RAGFlowProvider(settings, timeout_seconds=timeout_seconds)
