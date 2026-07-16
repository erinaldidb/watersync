from __future__ import annotations

from typing import Any


def normalize_text(value: str | None) -> str:
    return (value or "").strip()


def quote_sql_string(value: str) -> str:
    return value.replace("'", "''")


def normalize_ingestion_type(value: str | None) -> str:
    normalized = normalize_text(value or "incremental").lower() or "incremental"
    allowed = {"full", "incremental"}
    if normalized not in allowed:
        raise ValueError(f"Unsupported ingestion_type '{value}'. Expected one of {sorted(allowed)}")
    return normalized


def default_target_table_name(source_table_name: str) -> str:
    return f"staging_{normalize_text(source_table_name).split('.')[-1]}"


def resolve_target_table_name(raw_target_table_name: str | None, source_table_name: str) -> str:
    configured_name = normalize_text(raw_target_table_name)
    return configured_name or default_target_table_name(source_table_name)


def row_to_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    return dict(row)
