from __future__ import annotations

from typing import Any, Literal, NamedTuple


class PendingRecord(NamedTuple):
    record: dict[str, Any]
    delivery_tag: int


class Batch(NamedTuple):
    batch_key: str
    records: list[PendingRecord]


DeliveryAction = Literal["ack", "nack"]
