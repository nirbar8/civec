from __future__ import annotations

from typing import Any

import numpy as np


def attach_cosine_similarity(
    *,
    records: list[dict[str, Any]],
    query_vector: object,
    vector_column: str,
    distance_column: str = "_distance",
    similarity_column: str = "similarity",
    keep_vector: bool,
    requested_synthetic_columns: set[str],
) -> list[dict[str, Any]]:
    if not records:
        return records

    q = np.asarray(query_vector, dtype=np.float32).reshape(-1)
    q_norm = float(np.linalg.norm(q))

    for row in records:
        vector = row.get(vector_column)
        if vector is not None:
            v = np.asarray(vector, dtype=np.float32).reshape(-1)
            similarity = exact_cosine_similarity(q, q_norm, v)
            row[similarity_column] = similarity
            row[distance_column] = 1.0 - similarity
        else:
            distance = row.get(distance_column)
            if distance is not None:
                row[similarity_column] = cosine_similarity_from_distance(
                    float(distance)
                )
        if similarity_column not in requested_synthetic_columns:
            row.pop(similarity_column, None)
        if not keep_vector:
            row.pop(vector_column, None)
    return records


def exact_cosine_similarity(q: np.ndarray, q_norm: float, v: np.ndarray) -> float:
    v_norm = float(np.linalg.norm(v))
    if q_norm == 0.0 or v_norm == 0.0:
        return 0.0
    cosine_similarity = float(np.dot(q, v) / (q_norm * v_norm))
    return max(-1.0, min(1.0, cosine_similarity))


def cosine_similarity_from_distance(distance: float) -> float:
    return max(-1.0, min(1.0, 1.0 - distance))
