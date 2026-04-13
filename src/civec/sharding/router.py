from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from civec.models import VectorRecord
from civec.sharding.models import ShardProgressUpdate, ShardRecord


class ShardRegistry(Protocol):
    def get_or_create_active_shard(
        self,
        *,
        namespace_id: str,
        table_prefix: str,
    ) -> ShardRecord: ...

    def list_shards(self, namespace_ids: list[str]) -> list[str]: ...

    def list_all_shards(self) -> list[str]: ...

    def increment_rows(
        self,
        shard_id: str,
        delta: int,
        *,
        min_ts: datetime | None,
        max_ts: datetime | None,
    ) -> ShardProgressUpdate: ...

    def get_shard(self, shard_id: str) -> ShardRecord | None: ...


@dataclass(frozen=True)
class ShardRouterConfig:
    table_prefix: str = "vectors"


class ShardRouter:
    def __init__(
        self,
        db: Any,
        schema: Any,
        config: ShardRouterConfig,
        registry: ShardRegistry,
    ) -> None:
        self.db = db
        self.schema = schema
        self.cfg = config
        self._registry = registry

    def ensure_table_for_insert(self, table_name: str) -> Any:
        if table_name in self.db.table_names(limit=10_000):
            return self.db.open_table(table_name)
        return self.db.create_table(table_name, schema=self.schema)

    def open_tables_for_query(
        self,
        filters: dict[str, list[str]],
        start_ts: datetime,
        end_ts: datetime,
    ) -> list[Any]:
        candidate_names = set(self.table_names_for_query(filters, start_ts, end_ts))
        existing = set(self.db.list_tables().tables)
        table_names = sorted(candidate_names & existing)
        return [self.db.open_table(name) for name in table_names]

    def record_insert(
        self,
        table_name: str,
        rows_added: int,
        *,
        min_ts: datetime | None,
        max_ts: datetime | None,
    ) -> ShardProgressUpdate:
        return self._registry.increment_rows(
            table_name,
            rows_added,
            min_ts=min_ts,
            max_ts=max_ts,
        )

    def table_name_for_insert(self, record: VectorRecord) -> str:
        shard = self._registry.get_or_create_active_shard(
            namespace_id=record.namespace_id,
            table_prefix=self.cfg.table_prefix,
        )
        return shard.table_name

    def batch_key_for_insert(self, record: VectorRecord) -> str:
        return record.namespace_id

    def table_name_for_batch(self, batch_key: str) -> str:
        shard = self._registry.get_or_create_active_shard(
            namespace_id=batch_key,
            table_prefix=self.cfg.table_prefix,
        )
        return shard.table_name

    def table_names_for_query(
        self,
        filters: dict[str, list[str]],
        start_ts: datetime,
        end_ts: datetime,
    ) -> list[str]:
        namespace_ids = filters.get("namespace_id", [])
        if not namespace_ids:
            return self._registry.list_all_shards()
        return self._registry.list_shards(namespace_ids)

    def shard_record(self, table_name: str) -> ShardRecord | None:
        return self._registry.get_shard(table_name)
