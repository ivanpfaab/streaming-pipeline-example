"""Affinity keys for partitioning — one Kafka topic, not one topic per event type."""

from __future__ import annotations

from utils.common.csv_format import decode_record

CONVERSION_VALUES = {"1", "true", "yes", "y"}


def affinity_key(row: dict[str, str]) -> str:
    raw = (row.get("is_conversion") or "").strip().lower()
    if raw in CONVERSION_VALUES:
        return "conversion"
    return "page_view"


def affinity_from_csv_value(raw: str) -> str:
    _, row = decode_record(raw)
    return affinity_key(row)
