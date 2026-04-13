from __future__ import annotations

from typing import Mapping

import pyarrow as pa

from civec.models import MetadataFieldConfig, MetadataType

BASE_COLUMNS = ("id", "namespace_id", "source_timestamp", "ingested_at", "vector")
RESERVED_COLUMNS = set(BASE_COLUMNS) | {"_distance"}


def get_vector_schema(
    vector_dim: int,
    metadata_fields: Mapping[str, MetadataFieldConfig] | None = None,
) -> pa.Schema:
    vector_type = pa.list_(pa.float16(), vector_dim)
    fields: list[pa.Field] = [
        pa.field("id", pa.string()),
        pa.field("namespace_id", pa.string()),
        pa.field("source_timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
    ]

    for name, config in (metadata_fields or {}).items():
        _validate_metadata_column_name(name)
        fields.append(
            pa.field(
                name,
                _arrow_type_for_metadata(config.type),
                nullable=config.nullable,
            )
        )

    fields.append(pa.field("vector", vector_type))
    return pa.schema(fields)


def _arrow_type_for_metadata(type_name: MetadataType) -> pa.DataType:
    if type_name == "string":
        return pa.string()
    if type_name == "int64":
        return pa.int64()
    if type_name == "float64":
        return pa.float64()
    if type_name == "bool":
        return pa.bool_()
    if type_name == "timestamp":
        return pa.timestamp("us", tz="UTC")
    raise ValueError(f"unsupported metadata type: {type_name}")


def _validate_metadata_column_name(name: str) -> None:
    if not name:
        raise ValueError("metadata column name cannot be empty")
    if name in RESERVED_COLUMNS:
        raise ValueError(f"metadata column name '{name}' is reserved")
