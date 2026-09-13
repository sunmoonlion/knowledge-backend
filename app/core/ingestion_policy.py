"""Strict operator-owned mapping, independent of the retrieval allowlist."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetBinding:
    dataset_id: str
    dataset_name: str


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate ingestion binding field")
        result[key] = value
    return result


def parse_bindings(raw: str) -> dict[str, DatasetBinding]:
    try:
        if len(raw) > 65536:
            raise ValueError("binding configuration too large")
        data = json.loads(raw, object_pairs_hook=_unique_pairs)
        if not isinstance(data, dict) or len(data) > 128:
            raise ValueError("invalid binding map")
        result = {}
        ids = set()
        for key, value in data.items():
            if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,119}", key):
                raise ValueError("invalid dataset key")
            if not isinstance(value, dict) or set(value) != {
                "dataset_id",
                "dataset_name",
            }:
                raise ValueError("invalid binding fields")
            dataset_id, name = value["dataset_id"], value["dataset_name"]
            if not isinstance(dataset_id, str) or not re.fullmatch(
                r"[A-Za-z0-9_-]{1,128}", dataset_id
            ):
                raise ValueError("invalid provider id")
            if (
                not isinstance(name, str)
                or not 1 <= len(name) <= 128
                or name != name.strip()
                or any(ord(c) < 32 for c in name)
            ):
                raise ValueError("invalid provider name")
            if dataset_id in ids:
                raise ValueError("aliased provider id")
            ids.add(dataset_id)
            result[key] = DatasetBinding(dataset_id, name)
        return result
    except (ValueError, TypeError) as exc:
        raise ValueError("INGESTION_DATASET_BINDINGS is invalid") from exc
