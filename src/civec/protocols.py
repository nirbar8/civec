from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Mapping, Protocol, TypeVar

from civec.models import NamespaceRecord, VectorRecord

RawMessageT_contra = TypeVar("RawMessageT_contra", contravariant=True)
QueryInputT_contra = TypeVar("QueryInputT_contra", contravariant=True)


class RecordTransformer(Protocol[RawMessageT_contra]):
    def parse_records(
        self,
        message: RawMessageT_contra,
        now: datetime,
    ) -> list[VectorRecord]: ...


class QueryVectorProvider(Protocol[QueryInputT_contra]):
    def vector_for_query(self, query: QueryInputT_contra) -> list[float]: ...


class PartitionStrategy(Protocol):
    def batch_key_for_record(self, record: VectorRecord) -> str: ...

    def namespace_filter(self, namespace_id: str) -> Mapping[str, list[str]]: ...


class SchemaProvider(Protocol):
    def storage_schema(self) -> Any: ...


class NamespaceRegistry(Protocol):
    def get_namespace(self, namespace_id: str) -> NamespaceRecord | None: ...


class RuleEvaluator(Protocol):
    def matches(self, *, rule: object, document: Mapping[str, Any]) -> bool: ...


class TransportPublisher(Protocol):
    def publish(self, body: bytes) -> None: ...


class TransportConsumer(Protocol):
    def consume(self, handler: Callable[[bytes], None]) -> None: ...
