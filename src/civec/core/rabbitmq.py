import time
from typing import Optional, Protocol

import pika
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel

class RabbitmqPublisherSettings(Protocol):
    rabbitmq_username: str
    rabbitmq_password: str
    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_queue: str


class RabbitmqPublisher:
    def __init__(
        self,
        connection: pika.BlockingConnection,
        channel: BlockingChannel,
        queue_name: str,
        properties: BasicProperties,
        *,
        rate: float = 0.0,
    ) -> None:
        self._connection = connection
        self._channel = channel
        self._queue_name = queue_name
        self._properties = properties
        self._next_send = time.perf_counter()
        self._interval = 1.0 / rate if rate > 0 else 0.0

    @classmethod
    def from_settings(
        cls,
        settings: RabbitmqPublisherSettings,
        *,
        host: Optional[str] = None,
        queue: Optional[str] = None,
        persistent: bool = False,
        confirm_delivery: bool = False,
        rate: float = 0.0,
    ) -> "RabbitmqPublisher":
        credentials = pika.PlainCredentials(
            username=settings.rabbitmq_username,
            password=settings.rabbitmq_password,
        )
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host or settings.rabbitmq_host,
                port=settings.rabbitmq_port,
                credentials=credentials,
            )
        )
        channel = connection.channel()
        queue_name = queue or settings.rabbitmq_queue
        channel.queue_declare(queue=queue_name, durable=True)
        if confirm_delivery:
            try:
                channel.confirm_delivery()
            except Exception:
                pass
        properties = BasicProperties(
            content_type="application/json",
            delivery_mode=2 if persistent else None,
        )
        return cls(
            connection=connection,
            channel=channel,
            queue_name=queue_name,
            properties=properties,
            rate=rate,
        )

    @property
    def queue_name(self) -> str:
        return self._queue_name

    def publish(self, body: bytes) -> None:
        if self._interval > 0:
            now = time.perf_counter()
            if now < self._next_send:
                time.sleep(self._next_send - now)

        self._channel.basic_publish(
            exchange="",
            routing_key=self._queue_name,
            body=body,
            properties=self._properties,
        )

        if self._interval > 0:
            self._next_send = max(self._next_send + self._interval, time.perf_counter())

    def close(self) -> None:
        if self._connection.is_open:
            self._connection.close()
