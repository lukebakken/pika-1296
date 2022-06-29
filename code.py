import json
import traceback
import threading

import asyncio
import pika
from pika.adapters.asyncio_connection import AsyncioConnection


class RabbitMQConnection:
    def __init__(self, connection_string, on_open_callback, on_error_callback):
        self._on_open_callback = on_open_callback
        self._on_error_callback = on_error_callback

        # self._ioloop = asyncio.new_event_loop()
        self._ioloop = asyncio.get_event_loop()

        self._connection_string = connection_string
        self._connection = None
        self._connect()

        self._reconnect_delay = 0

    def _connect(self):
        self._connection = AsyncioConnection(
            parameters=pika.URLParameters(self._connection_string),
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
            custom_ioloop=self._ioloop,
        )

    def _on_connection_open(self, connection):
        self._on_open_callback(connection)
        self._reconnect_delay = 0

    def _on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        print("RabbitMQ Connection error", repr(err))
        self._on_error_callback(_unused_connection, err)
        self._stopping = False
        self._stop()

    def _on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._on_error_callback(_unused_connection, reason)
        self._stop()

    def start(self):
        def start_loop(loop):
            asyncio.set_event_loop(loop)
            while True:
                try:
                    loop.run_forever()
                    self._connection.ioloop.run_forever()
                except:
                    traceback.print_exc()

        thread = threading.Thread(target=start_loop, args=(self._ioloop,), daemon=True)
        thread.start()

    def stop(self):
        self._stopping = True
        self._stop()

    def _stop(self):
        self._connection.ioloop.stop()

        if self._connection.is_open:
            self._connection.close()

        if not self._stopping:
            self._ioloop.create_task(self._reconnect())
        else:
            # TODO: Close thread
            # asyncio.create_task()
            pass

    async def _reconnect(self):
        connection_delay = self._get_reconnect_delay()
        print("RabbitMQ Disconnected. Reconnecting in %d seconds." % (connection_delay))
        await asyncio.sleep(connection_delay)
        self._connect()

    def _get_reconnect_delay(self):
        self._reconnect_delay = min(self._reconnect_delay + 1, 30)
        return self._reconnect_delay


class RabbitMQ:
    def __init__(
        self,
        rabbit_host="localhost",
        rabbit_port=5672,
        rabbit_username="guest",
        rabbit_password="guest",
        queue="pika-1296",
    ):

        # Initialization
        self._queue = queue
        self._channel = None
        self._channel_open_callbacks = []

        connection_string = f"amqp://{rabbit_username}:{rabbit_password}@{rabbit_host}:{rabbit_port}/%2f"
        print("Connecting to " + connection_string)
        self._connection = RabbitMQConnection(
            connection_string, self._on_connection_open, self._on_connection_closed
        )

    def _on_connection_open(self, connection):
        connection.channel(on_open_callback=self._on_channel_open)

    def _on_connection_closed(self, _unused_connection, reason_or_err):
        self._channel = None

    def _on_channel_open(self, channel):
        self._channel = channel
        self._channel.queue_declare(queue=self.queue)

        for callback in self._channel_open_callbacks:
            callback(self._channel)
        self._channel_open_callbacks = []

    def start(self):
        self._connection.start()

    def stop(self):
        # TODO: Stop consuming
        self._connection.stop()

    def consume_keywords(self, callback):
        kwargs = dict(
            queue=self._queue_name, on_message_callback=callback, auto_ack=True
        )

        if not self._channel:
            self._channel_open_callbacks.append(
                lambda channel: channel.basic_consume(**kwargs)
            )
            return

        self._channel.basic_consume(**kwargs)

    def publish_keywords(self, keywords):
        keywords_list = list(iter(keywords))
        if len(keywords_list) == 0:
            return

        if self._channel:
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=json.dumps(keywords_list),
                properties=pika.BasicProperties(
                    content_type="application/json", delivery_mode=1
                ),
            )
