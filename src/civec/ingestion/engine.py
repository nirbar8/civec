from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, Protocol, TypeVar

from civec.ingestion.batching import BatchManager
from civec.ingestion.models import PendingRecord
from civec.models import VectorRecord
from civec.protocols import RecordTransformer

MessageT = TypeVar("MessageT")


class VectorRecordWriter(Protocol):
    def write_records(self, batch_key: str, records: list[VectorRecord]) -> None: ...


class VectorPartitioner(Protocol):
    def batch_key_for_record(self, record: VectorRecord) -> str: ...


@dataclass(frozen=True)
class IngestionEngineConfig:
    batch_size: int
    flush_interval_seconds: int


class IngestionEngine(Generic[MessageT]):
    def __init__(
        self,
        *,
        config: IngestionEngineConfig,
        transformer: RecordTransformer[MessageT],
        partitioner: VectorPartitioner,
        writer: VectorRecordWriter,
        logger,
    ) -> None:
        self._transformer = transformer
        self._partitioner = partitioner
        self._writer = writer
        self._batch_manager = BatchManager(
            batch_size=config.batch_size,
            flush_interval_seconds=config.flush_interval_seconds,
            logger=logger,
            flush_callback=self.flush_batch,
        )

    def start(self) -> None:
        self._batch_manager.start()

    def shutdown(self) -> None:
        self._batch_manager.shutdown()

    def ingest_message(
        self,
        *,
        message: MessageT,
        delivery_tag: int,
        now: datetime,
    ) -> int:
        records = self._transformer.parse_records(message, now)
        if not records:
            return 0

        rows: list[dict[str, Any]] = [
            _record_row(
                record=record,
                batch_key=self._partitioner.batch_key_for_record(record),
            )
            for record in records
        ]
        self._batch_manager.buffer_records(
            delivery_tag=delivery_tag,
            now=now,
            records=rows,
            batch_key_for_record=lambda row, _: str(row["batch_key"]),
        )
        return len(records)

    def flush_batch(
        self,
        batch_key: str,
        batch: list[PendingRecord],
        use_threadsafe_acks: bool,
    ) -> None:
        records = [
            VectorRecord(
                id=str(item.record["id"]),
                namespace_id=str(item.record["namespace_id"]),
                vector=[float(value) for value in item.record["vector"]],
                source_timestamp=item.record["source_timestamp"],
                ingested_at=item.record["ingested_at"],
                metadata=dict(item.record["metadata"]),
            )
            for item in batch
        ]
        self._writer.write_records(batch_key, records)


def _record_row(*, record: VectorRecord, batch_key: str) -> dict[str, Any]:
    return {
        "id": record.id,
        "namespace_id": record.namespace_id,
        "source_timestamp": record.source_timestamp,
        "ingested_at": record.ingested_at,
        "metadata": dict(record.metadata),
        "vector": list(record.vector),
        "batch_key": batch_key,
    }
