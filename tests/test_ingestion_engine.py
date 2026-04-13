from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from civec.ingestion.engine import IngestionEngine, IngestionEngineConfig
from civec.models import VectorRecord


@dataclass(frozen=True)
class Event:
    event_id: str
    namespace_id: str


class EventTransformer:
    def parse_records(self, message: Event, now: datetime) -> list[VectorRecord]:
        return [
            VectorRecord(
                id=message.event_id,
                namespace_id=message.namespace_id,
                vector=[1.0, 0.0],
                source_timestamp=None,
                ingested_at=now,
                metadata={"kind": "event"},
            )
        ]


class NamespacePartitioner:
    def batch_key_for_record(self, record: VectorRecord) -> str:
        return record.namespace_id


class CapturingWriter:
    def __init__(self) -> None:
        self.written: list[tuple[str, list[VectorRecord]]] = []

    def write_records(self, batch_key: str, records: list[VectorRecord]) -> None:
        self.written.append((batch_key, records))


def test_ingestion_engine_transforms_partitions_and_writes_records() -> None:
    writer = CapturingWriter()
    engine = IngestionEngine(
        config=IngestionEngineConfig(batch_size=1, flush_interval_seconds=0),
        transformer=EventTransformer(),
        partitioner=NamespacePartitioner(),
        writer=writer,
        logger=logging.getLogger("tests.civec.ingestion"),
    )
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    count = engine.ingest_message(
        message=Event(event_id="event-1", namespace_id="namespace-1"),
        delivery_tag=1,
        now=now,
    )
    engine.shutdown()

    assert count == 1
    assert writer.written == [
        (
            "namespace-1",
            [
                VectorRecord(
                    id="event-1",
                    namespace_id="namespace-1",
                    vector=[1.0, 0.0],
                    source_timestamp=None,
                    ingested_at=now,
                    metadata={"kind": "event"},
                )
            ],
        )
    ]
