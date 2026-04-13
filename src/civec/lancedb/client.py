from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Iterator, Mapping, Sequence

import numpy as np
import pyarrow as pa
from lancedb.table import Table


@dataclass(frozen=True)
class VectorBatch:
    ids: np.ndarray
    vectors: np.ndarray


@dataclass(frozen=True)
class TableScanConfig:
    id_column: str = "id"
    vector_column: str = "vector"
    where: str | None = None
    batch_size: int = 4096


@dataclass(frozen=True)
class UpsertConfig:
    id_column: str = "id"
    vector_column: str = "vector"
    metadata_columns: Sequence[str] = ()
    batch_size: int = 8192
    update_on_conflict: bool = True


@dataclass(frozen=True)
class IndexConfig:
    vector_column: str = "vector"
    metric: str = "cosine"
    num_partitions: int = 256
    num_sub_vectors: int = 32


@dataclass(frozen=True)
class TableCompareResult:
    left_rows: int
    right_rows: int
    intersection: int
    left_only: int
    right_only: int


class LanceDBClient:
    def __init__(self, *, database: str, db: Any) -> None:
        self._database = database
        self._db = db

    @property
    def database(self) -> str:
        return self._database

    def list_tables(self) -> list[str]:
        listed = self._db.list_tables()
        if hasattr(listed, "tables"):
            return sorted(listed.tables)
        return sorted(listed)

    def open_table(self, name: str) -> Table:
        return self._db.open_table(name)

    def iter_vector_batches(
        self,
        table: Table,
        config: TableScanConfig,
    ) -> Iterator[VectorBatch]:
        query = table.search().select([config.id_column, config.vector_column])
        if config.where:
            query = query.where(config.where)

        try:
            batches = query.to_batches(config.batch_size)
        except TypeError:
            batches = query.to_batches()

        for batch in batches:
            if batch.num_rows == 0:
                continue

            parsed = _parse_vector_batch(
                batch,
                config.id_column,
                config.vector_column,
            )

            if parsed.vectors.size == 0:
                continue

            yield parsed

    def upsert_vectors(
        self,
        table: Table,
        batch: VectorBatch,
        metadata: Mapping[str, np.ndarray] | None = None,
        config: UpsertConfig = UpsertConfig(),
    ) -> None:
        arrays: dict[str, pa.Array] = {
            config.id_column: pa.array(batch.ids),
            config.vector_column: pa.FixedSizeListArray.from_arrays(
                batch.vectors.reshape(-1),
                batch.vectors.shape[1],
            ),
        }

        if metadata:
            for key, values in metadata.items():
                arrays[key] = pa.array(values)

        record_batch = pa.RecordBatch.from_arrays(
            list(arrays.values()),
            list(arrays.keys()),
        )

        merge = table.merge_insert(on=config.id_column)

        if config.update_on_conflict:
            merge = merge.when_matched_update_all()

        merge.when_not_matched_insert_all().execute(record_batch)

    def compact_table(
        self,
        table: Table,
        *,
        cleanup_old_versions: bool = True,
    ) -> None:
        table.optimize()

        if cleanup_old_versions:
            table.cleanup_old_versions(timedelta(days=0))

    def table_stats(self, table: Table) -> dict[str, object]:
        return {
            "rows": table.count_rows(),
            "version": table.version,
            "indices": table.list_indices(),
        }

    def create_vector_index(
        self,
        table: Table,
        config: IndexConfig,
        *,
        replace: bool = False,
    ) -> None:
        if replace:
            try:
                table.drop_index(config.vector_column)
            except Exception:
                pass

        table.create_index(
            vector_column_name=config.vector_column,
            index_type="IVF_PQ",
            metric=config.metric,
            num_partitions=config.num_partitions,
            num_sub_vectors=config.num_sub_vectors,
        )

    def create_scalar_index(
        self,
        table: Table,
        column: str,
        *,
        replace: bool = False,
    ) -> None:
        if replace:
            try:
                table.drop_index(column)
            except Exception:
                pass

        table.create_scalar_index(column, replace=replace)

    def migrate_table(
        self,
        source: Table,
        target_db: "LanceDBClient",
        target_name: str,
        *,
        batch_size: int = 10_000,
        overwrite: bool = True,
    ) -> Table:
        mode = "overwrite" if overwrite else "create"

        target = target_db._db.create_table(
            target_name,
            schema=source.schema,
            mode=mode,
        )

        query = source.search()

        try:
            batches = query.to_batches(batch_size)
        except TypeError:
            batches = query.to_batches()

        for batch in batches:
            if batch.num_rows == 0:
                continue
            target.add(batch)

        return target

    def compare_tables(
        self,
        left: Table,
        right: Table,
        *,
        id_column: str = "id",
    ) -> TableCompareResult:
        left_df = left.search().select([id_column]).to_pandas()
        right_df = right.search().select([id_column]).to_pandas()

        left_ids = set(left_df[id_column].tolist())
        right_ids = set(right_df[id_column].tolist())

        intersection = left_ids & right_ids

        return TableCompareResult(
            left_rows=len(left_ids),
            right_rows=len(right_ids),
            intersection=len(intersection),
            left_only=len(left_ids - right_ids),
            right_only=len(right_ids - left_ids),
        )


def _parse_vector_batch(
    batch: pa.RecordBatch,
    id_column: str,
    vector_column: str,
) -> VectorBatch:
    id_idx = batch.schema.get_field_index(id_column)
    vec_idx = batch.schema.get_field_index(vector_column)

    if id_idx < 0 or vec_idx < 0:
        raise KeyError(
            f"Missing id/vector columns: {id_column}={id_idx} {vector_column}={vec_idx}"
        )

    id_col = batch.column(id_idx)
    vector_col = batch.column(vec_idx)

    if pa.types.is_fixed_size_list(vector_col.type) and vector_col.null_count == 0:
        list_size = vector_col.type.list_size
        values = vector_col.values.to_numpy(zero_copy_only=False)
        vectors = values.reshape(batch.num_rows, list_size)
        ids = id_col.to_numpy(zero_copy_only=False)
        return VectorBatch(ids=ids, vectors=vectors)

    vectors_raw = vector_col.to_numpy(zero_copy_only=False)
    ids_raw = id_col.to_numpy(zero_copy_only=False)

    vectors_list = []
    ids_list = []

    for record_id, vector in zip(ids_raw, vectors_raw):
        if vector is None:
            continue
        vectors_list.append(np.asarray(vector))
        ids_list.append(record_id)

    if not vectors_list:
        return VectorBatch(
            ids=np.asarray([]),
            vectors=np.empty((0, 0), dtype=np.float32),
        )

    vectors = np.stack(vectors_list).astype(np.float32, copy=False)
    ids = np.asarray(ids_list)

    return VectorBatch(ids=ids, vectors=vectors)
