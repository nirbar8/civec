from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Literal, Mapping

from pydantic import BaseModel, Field

MetadataType = Literal["string", "int64", "float64", "bool", "timestamp"]


class VectorMetric(str, Enum):
    COSINE = "cosine"
    L2 = "l2"
    DOT = "dot"


class MetadataFieldConfig(BaseModel):
    type: MetadataType = "string"
    nullable: bool = True


class VectorSchemaConfig(BaseModel):
    vector_dim: int = Field(ge=1)
    metadata_fields: dict[str, MetadataFieldConfig] = Field(default_factory=dict)


@dataclass(frozen=True)
class VectorRecord:
    id: str
    namespace_id: str
    vector: list[float]
    ingested_at: datetime
    source_timestamp: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ShardPolicy:
    table_prefix: str = "vectors"
    max_rows: int = 3_000_000


@dataclass(frozen=True)
class IndexPolicy:
    vector_index_type: str = "IVF_SQ"
    vector_index_metric: str = VectorMetric.COSINE.value
    vector_index_build_ratio: float = 0.9
    vector_index_num_partitions: int | None = None
    vector_index_num_sub_vectors: int | None = None
    vector_index_params: Mapping[str, Any] = field(default_factory=dict)
    auto_scalar_index_columns: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RetentionPolicy:
    rolling_shards: bool = True
    max_shards: int | None = None


@dataclass(frozen=True)
class NamespaceRecord:
    namespace_id: str
    archived: bool
    shard_policy: ShardPolicy
    index_policy: IndexPolicy
    retention_policy: RetentionPolicy
    fragment_maintenance_rows: int = 100_000
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None
    archived_at: str | None = None
