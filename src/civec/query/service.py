from __future__ import annotations

import heapq
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Protocol

import numpy as np
import pandas as pd

SYNTHETIC_RESULT_COLUMNS = {"similarity"}


class QueryShardRecord(Protocol):
    table_name: str
    min_ts_ingested: str | None
    max_ts_ingested: str | None


class QueryShardRegistry(Protocol):
    def list_shard_records(
        self,
        namespace_id: str,
    ) -> list[QueryShardRecord]: ...


@dataclass(frozen=True)
class QueryColumnConfig:
    namespace_column: str = "namespace_id"
    timestamp_column: str = "ingested_at"
    vector_column: str = "vector"


class GenericQueryService:
    def __init__(
        self,
        db_conn: Any,
        registry: QueryShardRegistry,
        logger: logging.Logger,
        *,
        stable_db_conn: Any | None = None,
        columns: QueryColumnConfig = QueryColumnConfig(),
        distance_metric: str = "cosine",
        ann_nprobes: int | None = None,
        ann_refine_factor: int | None = None,
        max_workers: int = 8,
    ) -> None:
        self._db = db_conn
        self._stable_db = stable_db_conn
        self._registry = registry
        self._logger = logger
        self._columns = columns
        self._distance_metric = distance_metric
        self._ann_nprobes = ann_nprobes
        self._ann_refine_factor = ann_refine_factor
        self._pool = ThreadPoolExecutor(max_workers=max_workers)

    def count_records(
        self,
        *,
        namespace_id: str,
        start_ts: datetime,
        end_ts: datetime,
        extra_where: str | None,
    ) -> tuple[int, list[str]]:
        tables = self.open_tables_for_namespace(namespace_id, start_ts, end_ts)
        if not tables:
            return 0, []

        where_clause = self.combine_where(
            namespace_id=namespace_id,
            start_ts=start_ts,
            end_ts=end_ts,
            extra=extra_where,
        )
        count_per_table = {
            table.name: int(table.count_rows(filter=where_clause)) for table in tables
        }
        counted = [table.name for table in tables]
        self._logger.info("count.done shards=%d total=%d", len(counted), sum(count_per_table.values()))
        return sum(count_per_table.values()), counted

    def search_by_vector(
        self,
        *,
        namespace_id: str,
        start_ts: datetime,
        end_ts: datetime,
        query_vector: object,
        k: int,
        where_clause: str | None,
        columns: list[str] | None,
        nprobes: int | None = None,
        return_distance: bool = True,
        force_vector_column: bool = False,
    ) -> list[dict[str, Any]]:
        records = self._registry.list_shard_records(namespace_id)
        records = [
            record
            for record in records
            if _overlaps_range(record, start_ts=start_ts, end_ts=end_ts)
        ]
        tables = _open_tables(
            primary_db=self._db,
            fallback_db=self._stable_db,
            table_names=[record.table_name for record in records],
        )
        if not tables:
            return []

        futures = [
            self._pool.submit(
                _search_one_table,
                table,
                query_vector,
                where_clause,
                k,
                columns,
                self._distance_metric,
                self._columns.vector_column,
                nprobes if nprobes is not None else self._ann_nprobes,
                self._ann_refine_factor,
                return_distance,
                force_vector_column,
            )
            for table in tables
        ]
        results: list[dict[str, Any]] = []
        for fut in as_completed(futures):
            try:
                results.extend(fut.result())
            except Exception:
                self._logger.exception("search.shard.failed")

        if not results:
            return []

        if return_distance and any("_distance" in row for row in results):
            return heapq.nsmallest(
                k,
                results,
                key=lambda row: row.get("_distance", float("inf")),
            )
        return results[:k]

    def open_tables_for_namespace(
        self,
        namespace_id: str,
        start_ts: datetime,
        end_ts: datetime,
    ) -> list[Any]:
        records = self._registry.list_shard_records(namespace_id)
        filtered = [
            record
            for record in records
            if _overlaps_range(record, start_ts=start_ts, end_ts=end_ts)
        ]
        return _open_tables(
            primary_db=self._db,
            fallback_db=self._stable_db,
            table_names=[record.table_name for record in filtered],
        )

    def combine_where(
        self,
        *,
        namespace_id: str,
        start_ts: datetime,
        end_ts: datetime,
        extra: str | None,
    ) -> str:
        start_iso = _ts_iso(start_ts)
        end_iso = _ts_iso(end_ts)
        clauses = [
            f"{self._columns.namespace_column} = '{namespace_id}'",
            f"{self._columns.timestamp_column} >= timestamp '{start_iso}'",
            f"{self._columns.timestamp_column} <= timestamp '{end_iso}'",
        ]
        if extra:
            clauses.append(extra)
        return " AND ".join(clauses)


def _ts_iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if value is None:
        return None
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _overlaps_range(
    record: QueryShardRecord,
    *,
    start_ts: datetime,
    end_ts: datetime,
) -> bool:
    min_ts = _parse_iso(record.min_ts_ingested)
    max_ts = _parse_iso(record.max_ts_ingested)
    if min_ts is not None and min_ts > end_ts:
        return False
    if max_ts is not None and max_ts < start_ts:
        return False
    return True


def _apply_distance_type(query: Any, distance_metric: str) -> Any:
    if not distance_metric:
        return query
    metric = distance_metric.lower()
    try:
        return query.distance_type(metric)
    except AttributeError:
        try:
            return query.metric(metric)
        except AttributeError:
            return query


def _apply_ann_params(
    query: Any,
    nprobes: int | None,
    refine_factor: int | None,
) -> Any:
    if nprobes is not None:
        try:
            query = query.nprobes(nprobes)
        except AttributeError:
            pass
    if refine_factor is not None:
        try:
            query = query.refine_factor(refine_factor)
        except AttributeError:
            pass
    return query


def _search_one_table(
    table: Any,
    query_vector: object,
    where_clause: str | None,
    k: int,
    columns: list[str] | None,
    distance_metric: str,
    vector_column: str,
    nprobes: int | None,
    refine_factor: int | None,
    return_distance: bool,
    force_vector_column: bool,
) -> list[dict[str, Any]]:
    try:
        query = table.search(query_vector, vector_column_name=vector_column)
    except TypeError:
        query = table.search(query_vector)

    query = _apply_distance_type(query, distance_metric)
    query = _apply_ann_params(query, nprobes=nprobes, refine_factor=refine_factor)
    if where_clause:
        query = query.where(where_clause)

    if columns is None:
        cols = [field.name for field in table.schema if field.name != vector_column]
    else:
        cols = [column for column in columns if column not in SYNTHETIC_RESULT_COLUMNS]
    if force_vector_column and vector_column not in cols:
        cols.append(vector_column)
    if return_distance and "_distance" not in cols:
        cols.append("_distance")
    query = query.select(cols)

    query = query.limit(k)
    df = query.to_pandas()
    if df.empty:
        return []
    rows = df.to_dict(orient="records")
    return [_json_safe_record(row) for row in rows]


def _open_tables(
    *,
    primary_db: Any,
    fallback_db: Any | None,
    table_names: list[str],
) -> list[Any]:
    if not table_names:
        return []

    primary_existing = _db_table_names(primary_db)

    tables: list[Any] = []
    for table_name in table_names:
        if table_name in primary_existing:
            tables.append(primary_db.open_table(table_name))
            continue
        if fallback_db is None:
            continue
        try:
            tables.append(fallback_db.open_table(table_name))
        except Exception:
            continue
    return tables


def _db_table_names(db: Any) -> set[str]:
    if hasattr(db, "table_names"):
        try:
            return set(db.table_names())
        except Exception:
            pass

    if hasattr(db, "list_tables"):
        try:
            listed = db.list_tables()
            if hasattr(listed, "tables"):
                return set(listed.tables)
            return set(listed)
        except Exception:
            pass

    return set()


def _json_safe_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_safe_value(value) for key, value in record.items()}


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, dict):
        return {key: _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_json_safe_value(item) for item in value)
    return value


def requested_synthetic_columns(columns: list[str] | None) -> set[str]:
    if columns is None:
        return {"similarity"}
    return {column for column in columns if column in SYNTHETIC_RESULT_COLUMNS}
