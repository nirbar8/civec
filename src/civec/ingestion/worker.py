from __future__ import annotations

import abc
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Generic, Protocol, TypeVar

from pydantic import BaseModel

from civec.ingestion.batching import BatchManager
from civec.ingestion.models import DeliveryAction, PendingRecord

MessageType = TypeVar("MessageType")
NamespaceType = TypeVar("NamespaceType")


class NonRetryableMessageError(ValueError):
    pass


class WorkerConfig(BaseModel):
    batch_size: int
    flush_interval_seconds: int


class WorkerRuntimeSettings(Protocol):
    rabbitmq_ack_on_flush: bool
    rabbitmq_prefetch: int
    rabbitmq_queue: str
    table_prefix: str


class WorkerConsumer(Protocol):
    def setup_consumer(
        self,
        queue: str,
        on_message_callback: Callable[[Any, Any, Any, bytes], None],
        durable: bool = True,
        prefetch_count: int = 64,
    ) -> None: ...

    def start_consuming(self) -> None: ...

    def stop_consuming(self) -> None: ...

    def close(self) -> None: ...

    def apply_delivery_action(
        self,
        delivery_tags: list[int],
        action: DeliveryAction,
        threadsafe: bool,
        *,
        requeue: bool = True,
    ) -> None: ...

    def nack(self, delivery_tag: int, requeue: bool = True) -> None: ...


class WorkerShardRouter(Protocol):
    def batch_key_for_insert(self, metadata: Any, ts: datetime) -> str: ...

    def table_name_for_batch(self, batch_key: str) -> str: ...

    def ensure_table_for_insert(self, table_name: str) -> Any: ...

    def record_insert(
        self,
        table_name: str,
        rows_added: int,
        *,
        min_ts: datetime | None,
        max_ts: datetime | None,
    ) -> Any: ...


class WorkerTelemetry(Protocol):
    def message_received(self) -> None: ...

    def message_skipped(self) -> None: ...

    def message_accepted(self, *, records_buffered: int) -> None: ...

    def message_rejected(self, *, count: int) -> None: ...

    def message_failed(self) -> None: ...

    def shutdown(self) -> None: ...


class ShardReplicator:
    def __init__(
        self,
        cache_db: Any,
        stable_db: Any,
        logger: logging.Logger,
        max_workers: int = 1,
    ) -> None:
        self._cache_db = cache_db
        self._stable_db = stable_db
        self._logger = logger
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()
        self._futures: dict[str, Future[bool]] = {}
        self._stable_tables: set[str] = set()
        if self._stable_db is not None:
            try:
                self._stable_tables = set(self._stable_db.list_tables().tables)
            except Exception:
                self._logger.exception("replicate.init.failed listing stable tables")

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True)

    def enqueue_replication(self, table_name: str) -> None:
        if self._stable_db is None:
            return
        with self._lock:
            if table_name in self._stable_tables or table_name in self._futures:
                return
            future = self._executor.submit(self._replicate_table, table_name)
            self._futures[table_name] = future
        future.add_done_callback(
            lambda done, name=table_name: self._finalize_replication(name, done)
        )

    def replicate_blocking(self, table_name: str) -> None:
        if self._stable_db is None:
            return

        with self._lock:
            if table_name in self._stable_tables:
                return
            future = self._futures.get(table_name)

        if future is not None:
            ok = bool(future.result())
            if not ok:
                self._logger.warning("replicate.missing table=%s", table_name)
            return

        ok = self._replicate_table(table_name)
        if not ok:
            self._logger.warning("replicate.missing table=%s", table_name)
            return
        with self._lock:
            self._stable_tables.add(table_name)

    def _finalize_replication(self, table_name: str, done: Future[bool]) -> None:
        try:
            ok = bool(done.result())
        except Exception:
            ok = False

        with self._lock:
            self._futures.pop(table_name, None)
            if ok:
                self._stable_tables.add(table_name)

    def _replicate_table(self, table_name: str) -> bool:
        assert self._stable_db is not None
        try:
            with self._lock:
                if table_name in self._stable_tables:
                    return True

            source = self._cache_db.open_table(table_name)
            query = source.search()
            try:
                batches = query.to_batches(10000)
            except TypeError:
                batches = query.to_batches()

            target = None
            for batch in batches:
                if batch.num_rows == 0:
                    continue
                if target is None:
                    target = self._stable_db.create_table(
                        table_name, data=batch, mode="create"
                    )
                else:
                    target.add(batch)
            if target is None:
                self._stable_db.create_table(
                    table_name, schema=source.schema, mode="create"
                )
            self._logger.info("replicate.done table=%s", table_name)
            return True
        except Exception:
            self._logger.exception("replicate.failed table=%s", table_name)
            return False


class BaseIngestionWorker(Generic[MessageType, NamespaceType], abc.ABC):
    def __init__(
        self,
        config: WorkerConfig,
        router: WorkerShardRouter,
        settings: WorkerRuntimeSettings,
        cache_db: Any,
        stable_db: Any,
        consumer: WorkerConsumer,
        logger: logging.Logger,
        telemetry: WorkerTelemetry,
    ) -> None:
        self.router = router
        self.config = config
        self._settings = settings
        self._cache_db = cache_db
        self._stable_db = stable_db
        self._logger = logger
        self._consumer = consumer
        self._replicator = ShardReplicator(cache_db, stable_db, logger)
        self._telemetry = telemetry
        self._batch_manager = BatchManager(
            batch_size=config.batch_size,
            flush_interval_seconds=config.flush_interval_seconds,
            logger=logger,
            flush_callback=self.flush_table,
        )

    @abc.abstractmethod
    def validate_request(self, body: bytes) -> MessageType: ...

    @abc.abstractmethod
    def process_message(self, msg: MessageType, delivery_tag: int) -> None: ...

    @abc.abstractmethod
    def handle_error(self, msg: MessageType, e: Exception): ...

    @abc.abstractmethod
    def _batch_key_for_record(self, record: dict[str, Any], now: datetime) -> str: ...

    @abc.abstractmethod
    def _require_namespace(self, namespace_id: str) -> NamespaceType: ...

    @abc.abstractmethod
    def _optimize_shard(self, table_name: str) -> None: ...

    @abc.abstractmethod
    def _ensure_vector_index(self, table_name: str, namespace: NamespaceType) -> None: ...

    @abc.abstractmethod
    def _handle_sealed_shard(
        self, namespace: NamespaceType, table_name: str
    ) -> None: ...

    def run(self, settings: WorkerRuntimeSettings) -> None:
        if (
            settings.rabbitmq_ack_on_flush
            and self.config.flush_interval_seconds <= 0
            and self.config.batch_size > settings.rabbitmq_prefetch
        ):
            raise ValueError(
                "Invalid ingestion settings: ack-on-flush with zero flush interval "
                "requires ingest_batch_size <= rabbitmq_prefetch, otherwise consumer "
                "stalls at prefetch limit. Either lower ingest_batch_size, increase "
                "rabbitmq_prefetch, set ingest_flush_interval_seconds > 0, or set "
                "rabbitmq_ack_on_flush=false."
            )

        self._consumer.setup_consumer(
            queue=settings.rabbitmq_queue,
            prefetch_count=settings.rabbitmq_prefetch,
            on_message_callback=self.__callback,
        )
        self._logger.info("worker.consume queue=%s", settings.rabbitmq_queue)
        self._batch_manager.start()
        try:
            self._consumer.start_consuming()
        except KeyboardInterrupt:
            self._logger.info("worker.shutdown reason=keyboard_interrupt")
        finally:
            self._graceful_exit()

    def flush_table(
        self,
        batch_key: str,
        batch: list[PendingRecord],
        use_threadsafe_acks: bool = True,
    ) -> None:
        if not batch:
            self._logger.info("flush.skip batch_key=%s reason=empty_batch", batch_key)
            return
        table_name = "<pending>"
        delivery_tags = [item.delivery_tag for item in batch]
        records = [item.record for item in batch]
        try:
            namespace = self._require_namespace(batch_key)
            table_name = self.router.table_name_for_batch(batch_key)
            self._logger.info(
                "flush.start batch_key=%s table=%s batch=%d",
                batch_key,
                table_name,
                len(batch),
            )
            self.router.record_insert(
                table_name,
                0,
                min_ts=None,
                max_ts=None,
            )
            table = self.router.ensure_table_for_insert(table_name)
            table.add(records)

            min_ts, max_ts = _min_max_ts(records)
            progress = self.router.record_insert(
                table_name,
                len(batch),
                min_ts=min_ts,
                max_ts=max_ts,
            )

            if progress.crossed_fragment_boundary:
                self._optimize_shard(table_name)

            if progress.should_build_vector_index:
                self._ensure_vector_index(table_name, namespace)

            if progress.became_sealed:
                self._handle_sealed_shard(namespace, table_name)

        except NonRetryableMessageError as exc:
            self._logger.error(
                "flush.rejected batch_key=%s table=%s batch=%d reason=%s",
                batch_key,
                table_name,
                len(batch),
                exc,
            )
            if self._settings.rabbitmq_ack_on_flush:
                self._consumer.apply_delivery_action(
                    delivery_tags,
                    "nack",
                    threadsafe=use_threadsafe_acks,
                    requeue=False,
                )
            self._telemetry.message_rejected(count=len(delivery_tags))
            return
        except Exception:
            self._logger.exception(
                "flush.failed batch_key=%s table=%s batch=%d",
                batch_key,
                table_name,
                len(batch),
            )
            if self._settings.rabbitmq_ack_on_flush:
                self._consumer.apply_delivery_action(
                    delivery_tags, "nack", threadsafe=use_threadsafe_acks
                )
            raise
        else:
            if self._settings.rabbitmq_ack_on_flush:
                self._consumer.apply_delivery_action(
                    delivery_tags, "ack", threadsafe=use_threadsafe_acks
                )
        self._logger.info("flush.done batch_key=%s table=%s", batch_key, table_name)

    def __callback(
        self,
        _: Any,
        method: Any,
        __: Any,
        body: bytes,
    ) -> None:
        msg = None
        try:
            msg = self.validate_request(body)
            self.process_message(msg, method.delivery_tag)
        except NonRetryableMessageError:
            self._telemetry.message_rejected(count=1)
            self._logger.error("message.rejected delivery_tag=%s", method.delivery_tag)
            self._consumer.nack(method.delivery_tag, requeue=False)
        except Exception as exc:
            self._telemetry.message_failed()
            if msg is not None:
                self.handle_error(msg, exc)
            else:
                self._logger.exception(
                    "message.failed delivery_tag=%s reason=decode",
                    method.delivery_tag,
                )
            self._consumer.nack(method.delivery_tag, requeue=True)

    def _graceful_exit(self) -> None:
        try:
            self._consumer.stop_consuming()
        except Exception:
            self._logger.exception("worker.stop_consuming.failed")
        self._batch_manager.shutdown()
        self._replicator.shutdown()
        self._telemetry.shutdown()
        self._consumer.close()


def _min_max_ts(records: list[dict[str, Any]]) -> tuple[datetime | None, datetime | None]:
    if not records:
        return None, None
    values = [
        value
        for value in (record.get("ts_ingested") for record in records)
        if isinstance(value, datetime)
    ]
    if not values:
        return None, None
    return min(values), max(values)
