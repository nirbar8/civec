import pickle
from datetime import datetime, timezone
from typing import Literal, Optional, Union
from uuid import uuid4

import numpy as np
from pydantic import BaseModel, ConfigDict, field_serializer, field_validator


class WorkerMessage(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: str
    metadata: dict
    results: list[np.ndarray]

    @field_validator("results", mode="before")
    @classmethod
    def _coerce_results_to_ndarray(cls, value):
        if value is None:
            return []
        if not isinstance(value, list):
            raise TypeError("results must be a list")

        converted: list[np.ndarray] = []
        for item in value:
            if isinstance(item, np.ndarray):
                converted.append(item.astype(np.float32, copy=False))
            else:
                converted.append(np.asarray(item, dtype=np.float32))
        return converted

    @field_serializer("results")
    def _serialize_results(self, value: list[np.ndarray]) -> list[list[float]]:
        return [np.asarray(item, dtype=np.float32).tolist() for item in value]

    def to_bytes(self):
        return pickle.dumps(
            self.model_dump(mode="json"), protocol=pickle.HIGHEST_PROTOCOL
        )

    @classmethod
    def from_bytes(cls, x: bytes) -> "WorkerMessage":
        return cls.model_validate(pickle.loads(x))
