from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from civec.models import VectorRecord
from civec.protocols import RecordTransformer
from civec.records import vector_record_to_storage_row


@dataclass(frozen=True)
class SensorMessage:
    event_id: str
    device_id: str
    embedding: list[float]
    captured_at: datetime
    label: str


class SensorTransformer:
    def parse_records(self, message: SensorMessage, now: datetime) -> list[VectorRecord]:
        return [
            VectorRecord(
                id=message.event_id,
                namespace_id=message.device_id,
                vector=message.embedding,
                source_timestamp=message.captured_at,
                ingested_at=now,
                metadata={"label": message.label},
            )
        ]


def test_non_image_transformer_uses_generic_record_model() -> None:
    now = datetime(2026, 2, 1, tzinfo=timezone.utc)
    captured_at = datetime(2026, 1, 31, tzinfo=timezone.utc)
    transformer: RecordTransformer[SensorMessage] = SensorTransformer()

    records = transformer.parse_records(
        SensorMessage(
            event_id="event-1",
            device_id="device-1",
            embedding=[0.1, 0.2, 0.3],
            captured_at=captured_at,
            label="vehicle",
        ),
        now,
    )

    assert vector_record_to_storage_row(records[0]) == {
        "id": "event-1",
        "namespace_id": "device-1",
        "source_timestamp": captured_at,
        "ingested_at": now,
        "label": "vehicle",
        "vector": [0.1, 0.2, 0.3],
    }
