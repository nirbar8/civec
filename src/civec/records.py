from __future__ import annotations

from typing import Any

from civec.models import VectorRecord


def vector_record_to_storage_row(record: VectorRecord) -> dict[str, Any]:
    row = {
        "id": record.id,
        "namespace_id": record.namespace_id,
        "ingested_at": record.ingested_at,
        "source_timestamp": record.source_timestamp,
        "vector": list(record.vector),
    }
    row.update(dict(record.metadata))
    return row
