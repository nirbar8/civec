from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class ShardState(str, Enum):
    ACTIVE = "active"
    SEALED = "sealed"
    DROPPED = "dropped"


@dataclass(frozen=True)
class ShardRecord:
    shard_id: str
    table_name: str
    namespace_id: str
    shard_seq: int
    row_count: int
    max_rows: int
    state: ShardState | str
    opened_at: str
    sealed_at: str | None = None
    dropped_at: str | None = None
    min_ingested_at: str | None = None
    max_ingested_at: str | None = None
    vector_indexed_at: str | None = None
    scalar_indexed_at: str | None = None
    vector_index_requested: bool = False
    last_fragment_maintenance_row: int = 0


@dataclass(frozen=True)
class ShardProgressUpdate:
    namespace_id: str
    row_count: int
    max_rows: int
    became_sealed: bool
    crossed_fragment_boundary: bool
    should_build_vector_index: bool


@dataclass(frozen=True)
class InsertProgress:
    table_name: str
    progress: ShardProgressUpdate
    min_ingested_at: datetime | None = None
    max_ingested_at: datetime | None = None
