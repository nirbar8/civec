from __future__ import annotations

import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Event, RLock, Thread
from typing import Any, Callable

from civec.ingestion.models import Batch, PendingRecord


class BatchManager:
    def __init__(
        self,
        batch_size: int,
        flush_interval_seconds: int,
        logger: logging.Logger,
        flush_callback: Callable[[str, list[PendingRecord], bool], None],
    ) -> None:
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self._logger = logger
        self._flush_callback = flush_callback

        self._batches: dict[str, list[PendingRecord]] = defaultdict(list)
        self._batch_first_seen: dict[str, datetime] = {}
        self._locks: dict[str, RLock] = defaultdict(RLock)

        self._flush_worker = ThreadPoolExecutor(max_workers=1)
        self._flush_stop = Event()
        self._flush_thread = Thread(target=self._flush_loop, daemon=True)

    def start(self) -> None:
        self._flush_thread.start()

    def shutdown(self) -> None:
        self._flush_stop.set()
        if self._flush_thread.is_alive():
            self._flush_thread.join()
        self._drain_batches()
        self._flush_worker.shutdown(wait=True)

    def buffer_records(
        self,
        delivery_tag: int,
        now: datetime,
        records: list[dict[str, Any]],
        batch_key_for_record: Callable[[dict[str, Any], datetime], str],
    ) -> None:
        scheduled: list[Batch] = []
        batch_key_records: dict[str, list[PendingRecord]] = defaultdict(list)
        for record in records:
            batch_key = batch_key_for_record(record, now)
            pending = PendingRecord(record=record, delivery_tag=delivery_tag)
            batch_key_records[batch_key].append(pending)

        for batch_key, records_for_key in batch_key_records.items():
            with self._locks[batch_key]:
                self._batches[batch_key].extend(records_for_key)
                if batch_key not in self._batch_first_seen:
                    self._batch_first_seen[batch_key] = now
                batch_size = len(self._batches[batch_key])

            if batch_size >= self.batch_size:
                scheduled.append(self._pop_batch(batch_key))

        self._submit_batches(scheduled)

    def _flush_loop(self) -> None:
        if self.flush_interval_seconds <= 0:
            return
        while not self._flush_stop.is_set():
            self._flush_stop.wait(timeout=self.flush_interval_seconds)
            if self._flush_stop.is_set():
                break
            self._flush_due_batches(datetime.now(timezone.utc))

    def _pop_batch(self, batch_key: str) -> Batch:
        with self._locks[batch_key]:
            batch = self._batches.pop(batch_key)
            self._batch_first_seen.pop(batch_key, None)
            return Batch(batch_key, batch)

    def _pop_batch_if_due(
        self,
        batch_key: str,
        now: datetime | None = None,
    ) -> Batch | None:
        with self._locks[batch_key]:
            if now is not None and not self._is_due_batch(batch_key, now):
                return None

            records = self._batches.pop(batch_key, [])
            self._batch_first_seen.pop(batch_key, None)
            if not records:
                return None

            return Batch(batch_key, records)

    def _submit_batches(self, scheduled: list[Batch]) -> None:
        for batch in scheduled:
            self._logger.info(
                "batch.flush.scheduled batch_key=%s batch=%d",
                batch.batch_key,
                len(batch.records),
            )
            self._flush_worker.submit(
                self._flush_callback,
                batch.batch_key,
                batch.records,
                True,
            )

    def _flush_due_batches(self, now: datetime) -> None:
        due_batches = self._collect_batches(now)
        for batch_key, batch in due_batches:
            self._logger.info(
                "batch.flush.interval batch_key=%s batch=%d",
                batch_key,
                len(batch),
            )
            self._flush_worker.submit(self._flush_callback, batch_key, batch, True)

    def _is_due_batch(self, batch_key: str, now: datetime) -> bool:
        first_seen = self._batch_first_seen.get(batch_key)
        if first_seen is None:
            return False

        seconds_since_seen = (now - first_seen).total_seconds()
        return seconds_since_seen >= self.flush_interval_seconds

    def _drain_batches(self) -> None:
        drained_batches = self._collect_batches()
        for batch_key, batch in drained_batches:
            self._logger.info(
                "batch.flush.shutdown batch_key=%s batch=%d",
                batch_key,
                len(batch),
            )
            self._flush_callback(batch_key, batch, False)

    def _collect_batches(self, now: datetime | None = None) -> list[Batch]:
        batches: list[Batch] = []
        for batch_key in list(self._batches.keys()):
            batch = self._pop_batch_if_due(batch_key, now)
            if batch is not None:
                batches.append(batch)
        return batches
