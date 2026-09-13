"""One ingestion policy shared by Admin, Internal, workers and receipt recovery."""

from __future__ import annotations

from app.application.errors.exceptions import ForbiddenError
from app.infrastructure.external.knowledge_provider import provider_definition
from app.infrastructure.models.knowledge import KnowledgeIngestionJob
from core.config import Settings
from core.ingestion_policy import DatasetBinding, parse_bindings

SNAPSHOT_KEY = "ingestion_binding_v1"


def resolve_binding(settings: Settings, dataset_key: str | None) -> DatasetBinding:
    binding = parse_bindings(settings.ingestion_dataset_bindings).get(dataset_key or "")
    if binding is None:
        raise ForbiddenError(
            "ingestion dataset is not authorized", code="ingestion_dataset_denied"
        )
    return binding


def binding_snapshot(settings: Settings, dataset_key: str | None) -> dict:
    binding = resolve_binding(settings, dataset_key)
    return {
        "dataset_key": dataset_key,
        "dataset_id": binding.dataset_id,
        "dataset_name": binding.dataset_name,
        "provider_base_sha256": provider_definition(settings).endpoint_fingerprint,
    }


def require_job_binding(
    job: KnowledgeIngestionJob, settings: Settings
) -> DatasetBinding:
    expected = binding_snapshot(settings, job.target_dataset)
    # Only the server-created FIRST acceptance entry is trusted. Client document
    # metadata and later Admin status journal entries cannot authorize a job.
    history = job.status_history or []
    first = history[0] if history and isinstance(history[0], dict) else {}
    metadata = first.get("metadata") or {}
    snapshot = metadata.get(SNAPSHOT_KEY) if isinstance(metadata, dict) else None
    if first.get("status") != "accepted" or snapshot != expected:
        raise ForbiddenError(
            "ingestion binding is missing or changed", code="ingestion_binding_changed"
        )
    return resolve_binding(settings, job.target_dataset)
