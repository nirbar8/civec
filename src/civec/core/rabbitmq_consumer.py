import logging
from functools import partial
from typing import Callable, Protocol

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from civec.ingestion.models import DeliveryAction

RabbitmqCallback = Callable[
    [BlockingChannel, Basic.Deliver, BasicProperties, bytes], None
]


class RabbitmqConsumerSettings(Protocol):
    rabbitmq_username: str
    rabbitmq_password: str
    rabbitmq_host: str
    rabbitmq_port: int


class RabbitmqConsumer:
    def __init__(
        self,
        connection: pika.BlockingConnection,
        channel: BlockingChannel,
        logger: logging.Logger,
    ) -> None:
        self._connection = connection
        self._channel = channel
        self._logger = logger

    @classmethod
    def from_settings(
        cls, settings: RabbitmqConsumerSettings, logger: logging.Logger
    ) -> "RabbitmqConsumer":
        conn_params = pika.ConnectionParameters(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            credentials=pika.PlainCredentials(
                username=settings.rabbitmq_username, password=settings.rabbitmq_password
            ),
        )
        connection = pika.BlockingConnection(conn_params)
        channel = connection.channel()
        return cls(connection, channel, logger)

    @property
    def channel(self) -> BlockingChannel:
        return self._channel

    def add_callback_threadsafe(self, callback: Callable[[], None]) -> None:
        self._connection.add_callback_threadsafe(callback)

    def ack(self, delivery_tag: int) -> None:
        self._channel.basic_ack(delivery_tag=delivery_tag)

    def nack(self, delivery_tag: int, requeue: bool = True) -> None:
        self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def setup_consumer(
        self,
        queue: str,
        on_message_callback: RabbitmqCallback,
        durable: bool = True,
        prefetch_count: int = 64,
    ) -> None:
        self._channel.queue_declare(queue=queue, durable=durable)
        self._channel.basic_qos(prefetch_count=prefetch_count)
        self._channel.basic_consume(
            queue=queue, on_message_callback=on_message_callback
        )

    def start_consuming(self) -> None:
        self._channel.start_consuming()

    def stop_consuming(self) -> None:
        if self._is_channel_open():
            self._channel.stop_consuming()

    def close(self) -> None:
        if self._is_connection_open():
            self._connection.close()

    def _is_channel_open(self) -> bool:
        return bool(getattr(self._channel, "is_open", True))

    def _is_connection_open(self) -> bool:
        return bool(getattr(self._connection, "is_open", True))

    def apply_delivery_action(
        self,
        delivery_tags: list[int],
        action: DeliveryAction,
        threadsafe: bool,
        *,
        requeue: bool = True,
    ) -> None:
        if not delivery_tags:
            return
        delivery_tags = list(dict.fromkeys(delivery_tags))

        if threadsafe:
            self.add_callback_threadsafe(
                partial(self._apply_delivery_action, delivery_tags, action, requeue, True)
            )
            return

        self._apply_delivery_action(delivery_tags, action, requeue, False)

    def _apply_delivery_action(
        self,
        delivery_tags: list[int],
        action: DeliveryAction,
        requeue: bool,
        log_errors: bool,
    ) -> None:
        try:
            for tag in delivery_tags:
                if action == "ack":
                    self.ack(delivery_tag=tag)
                else:
                    self.nack(delivery_tag=tag, requeue=requeue)
        except Exception:
            if not log_errors:
                raise
            self._logger.exception("%s.failed count=%d", action, len(delivery_tags))
