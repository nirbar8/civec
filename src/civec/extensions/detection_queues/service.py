from __future__ import annotations

import math

from civec.extensions.detection_queues.models import (
    ActiveQueueItem,
    DetectionMetric,
    TopKDecision,
    TopKQueue,
    VectorCandidate,
)


class TopKQueueScorer:
    def evaluate_candidate(
        self,
        *,
        queue: TopKQueue,
        candidate: VectorCandidate,
        active_items: list[ActiveQueueItem],
    ) -> TopKDecision:
        score = score_candidate(candidate.vector, queue.query_vector, queue.metric)
        if queue.min_score is not None and score < queue.min_score:
            return TopKDecision(score=score, should_insert=False)

        if len(active_items) < queue.limit:
            return TopKDecision(score=score, should_insert=True)

        lowest = min(active_items, key=lambda item: item.score)
        if score <= lowest.score:
            return TopKDecision(score=score, should_insert=False)
        return TopKDecision(
            score=score,
            should_insert=True,
            evict_item_id=lowest.item_id,
        )


def score_candidate(
    vector: list[float],
    query_vector: list[float],
    metric: DetectionMetric | str,
) -> float:
    metric_value = metric.value if isinstance(metric, DetectionMetric) else metric
    if metric_value != DetectionMetric.COSINE.value:
        raise ValueError(f"unsupported queue metric: {metric_value}")
    if len(vector) != len(query_vector):
        raise ValueError(
            f"vector length mismatch: candidate={len(vector)} queue={len(query_vector)}"
        )
    return cosine_similarity(vector, query_vector)


def cosine_similarity(left: list[float], right: list[float]) -> float:
    dot = 0.0
    left_norm = 0.0
    right_norm = 0.0
    for left_value, right_value in zip(left, right):
        dot += left_value * right_value
        left_norm += left_value * left_value
        right_norm += right_value * right_value
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return dot / (math.sqrt(left_norm) * math.sqrt(right_norm))
