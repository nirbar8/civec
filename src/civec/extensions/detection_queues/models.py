from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class DetectionMetric(str, Enum):
    COSINE = "cosine"


@dataclass(frozen=True)
class TopKQueue:
    queue_id: str
    limit: int
    query_vector: list[float]
    metric: DetectionMetric | str = DetectionMetric.COSINE
    min_score: float | None = None


@dataclass(frozen=True)
class VectorCandidate:
    source_event_id: str
    source_id: str
    namespace_id: str
    vector: list[float]


@dataclass(frozen=True)
class ActiveQueueItem:
    item_id: str
    score: float


@dataclass(frozen=True)
class TopKDecision:
    score: float
    should_insert: bool
    evict_item_id: str | None = None
