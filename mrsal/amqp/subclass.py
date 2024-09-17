from functools import partial
import pika
import json
from mrsal.exceptions import MrsalAbortedSetup
from logging import WARNING
from pika.connection import SSLOptions
from pika.exceptions import (
        AMQPConnectionError,
        ChannelClosedByBroker,
        StreamLostError,
        ConnectionClosedByBroker,
        NackError,
        UnroutableError
        )
from pika.adapters.asyncio_connection import AsyncioConnection
from typing import Any, Callable, Type
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_sleep_log
from pydantic import ValidationError
from pydantic.dataclasses import dataclass
from neolibrary.monitoring.logger import NeoLogger

from mrsal.superclass import Mrsal
from mrsal import config

log = NeoLogger(__name__, rotate_days=config.LOG_DAYS)

@dataclass
class MrsalAMQP(Mrsal):
    """
    :param int blocked_connection_timeout: blocked_connection_timeout
        is the timeout, in seconds,
        for the connection to remain blocked; if the timeout expires,
            the connection will be torn down during connection tuning.
    """
    blocked_connection_timeout: int = 60  # sec
    use_blocking: bool = False

    def get_ssl_context(self) -> SSLOptions | None:
        if self.ssl:
            self.log.info("Setting up TLS connection")
            context = self._ssl_setup()
        ssl_options = pika.SSLOptions(context, self.host) if 'context' in locals() else None
        return ssl_options

    def setup_blocking_connection(self) -> None:
        """We can use setup_blocking_connection for establishing a connection to RabbitMQ server specifying connection parameters.
        The connection is blocking which is only advisable to use for the apps with low througput. 

        DISCLAIMER: If you expect a lot of traffic to the app or if its realtime then you should use async.

        Parameters
        ----------
        context : Dict[str, str]
            context is the structured map with information regarding the SSL options for connecting with rabbit server via TLS.
        """
        connection_info = f"""
                            Mrsal connection parameters:
                            host={self.host},
                            virtual_host={self.virtual_host},
                            port={self.port},
                            heartbeat={self.heartbeat},
                            ssl={self.ssl}
                            """
        if self.verbose:
            self.log.info(f"Establishing connection to RabbitMQ on {connection_info}")
        credentials = pika.PlainCredentials(*self.credentials)
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    ssl_options=self.get_ssl_context(),
                    virtual_host=self.virtual_host,
                    credentials=credentials,
                    heartbeat=self.heartbeat,
                    blocked_connection_timeout=self.blocked_connection_timeout,
                )
            )

            self._channel = self._connection.channel()
            # Note: prefetch is set to 1 here as an example only.
            # In production you will want to test with different prefetch values to find which one provides the best performance and usability for your solution.
            # use a high number of prefecth if you think the pods with Mrsal installed can handle it. A prefetch 4 will mean up to 4 async runs before ack is required
            self._channel.basic_qos(prefetch_count=self.prefetch_count)
            self.log.info(f"Boom! Connection established with RabbitMQ on {connection_info}")
        except (AMQPConnectionError, ChannelClosedByBroker, ConnectionClosedByBroker, StreamLostError) as e:
            self.log.error(f"I tried to connect with the RabbitMQ server but failed with: {e}")
            raise
        except Exception as e:
            self.log.error(f"Unexpected error caught: {e}")

    def setup_async_connection(self) -> None:
        """We can use setup_aync_connection for establishing a connection to RabbitMQ server specifying connection parameters.
        The connection is async and is recommended to use if your app is realtime or will handle a lot of traffic.

        Parameters
        ----------
        context : Dict[str, str]
            context is the structured map with information regarding the SSL options for connecting with rabbit server via TLS.
        """
        connection_info = f"""
                            Mrsal connection parameters:
                            host={self.host},
                            virtual_host={self.virtual_host},
                            port={self.port},
                            heartbeat={self.heartbeat},
                            ssl={self.ssl}
                            """
        if self.verbose:
            self.log.info(f"Establishing connection to RabbitMQ on {connection_info}")
        credentials = pika.PlainCredentials(*self.credentials)
        conn_conf = pika.ConnectionParameters(
                                host=self.host,
                                port=self.port,
                                ssl_options=self.get_ssl_context(),
                                virtual_host=self.virtual_host,
                                credentials=credentials,
                                heartbeat=self.heartbeat,
                                )
        try:
            self._connection = AsyncioConnection(
                    parameters=conn_conf,
                    on_open_callback=partial(
                        self.on_connection_open,
                        exchange_name=self.exchange_name, queue_name=self.queue_name,
                        exchange_type=self.exchange_type, routing_key=self.routing_key
                        ),
                    on_open_error_callback=self.on_connection_error
                    )
            self.log.info(f"Connection staged with RabbitMQ on {connection_info}")
        except (AMQPConnectionError, ChannelClosedByBroker, ConnectionClosedByBroker, StreamLostError) as e:
            self.log.error(f"Oh lordy lord I failed connecting to the Rabbit with: {e}")
            raise
        except Exception as e:
            self.log.error(f"Unexpected error caught: {e}")



    @retry(
        retry=retry_if_exception_type((
            AMQPConnectionError,
            ChannelClosedByBroker,
            ConnectionClosedByBroker,
            StreamLostError,
            )),
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        before_sleep=before_sleep_log(log, WARNING)
           )
    def start_consumer(self,
                     queue_name: str,
                     callback: Callable | None = None,
                     callback_args: dict[str, str | int | float | bool] | None = None,
                     auto_ack: bool = True,
                     inactivity_timeout: int | None = None,  # just let conusmer wait patiently damn it
                     auto_declare: bool = True,
                     exchange_name: str | None = None,
                     exchange_type: str | None = None,
                     routing_key: str | None = None,
                     payload_model: Type | None = None
                     ) -> None:
        """
        Start the consumer using blocking setup.
        :param queue: The queue to consume from.
        :param auto_ack: If True, messages are automatically acknowledged.
        :param inactivity_timeout: Timeout for inactivity in the consumer loop.
        :param callback: The callback function to process messages.
        :param callback_args: Optional arguments to pass to the callback.
        """
        # Connect and start the I/O loop
        if self.use_blocking:
            self.setup_blocking_connection()
        else:
            # set connection parameters
            # parametes propagate through a 3 layers in order
            # to spin up the async connection
            self.queue_name = queue_name
            self.exchange_name = exchange_name
            self.exchange_type = exchange_type
            self.routing_key = routing_key
            self.auto_declare = auto_declare

            self.setup_async_connection()
            if self._connection.is_open:
                self.log.success(f"Boom! Async connection established with {exchange_name} on {queue_name}")
                self._connection.ioloop.run_forever()
            else:
                self.log.error('Straigh out of the swamp with no connection! Async connection did not activate')

        if auto_declare and self.use_blocking:
            if None in (exchange_name, queue_name, exchange_type, routing_key):
                raise TypeError('Make sure that you are passing in all the necessary args for auto_declare')
            self._setup_exchange_and_queue(
                    exchange_name=exchange_name,
                    queue_name=queue_name,
                    exchange_type=exchange_type,
                    routing_key=routing_key
                    )
            if not self.auto_declare_ok:
                if self._connection.is_open:
                    self._connection.ioloop.stop()
                raise MrsalAbortedSetup('Auto declaration for the connection setup failed and is aborted')

        self.log.info(f"Consumer boi listening on queue: {queue_name} to the exchange {exchange_name}. Waiting for messages...")

        try:
            for method_frame, properties, body in self._channel.consume(
                                queue=queue_name, auto_ack=auto_ack, inactivity_timeout=inactivity_timeout):
                if method_frame:
                    if properties:
                        app_id = properties.app_id if hasattr(properties, 'app_id') else 'no AppID given'
                        msg_id = properties.msg_id if hasattr(properties, 'msg_id') else 'no msgID given'

                    if self.verbose:
                        self.log.info(
                                """
                                Message received with:
                                - Method Frame: {method_frame)
                                - Redelivery: {method_frame.redelivered}
                                - Exchange: {method_frame.exchange}
                                - Routing Key: {method_frame.routing_key}
                                - Delivery Tag: {method_frame.delivery_tag}
                                - Properties: {properties}
                                """
                                )
                    if auto_ack:
                        self.log.info(f'I successfully received a message from: {app_id} with messageID: {msg_id}')
                    
                    if payload_model:
                        try:
                            self.validate_payload(body, payload_model)
                        except (ValidationError, json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
                            self.log.error(f"Oh lordy lord, payload validation failed for your specific model requirements: {e}")
                            if not auto_ack:
                                self._channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                            continue

                    if callback:
                        try:
                            if callback_args:
                                callback(*callback_args, method_frame, properties, body)
                            else:
                                callback( method_frame, properties, body)
                        except Exception as e:
                            if not auto_ack:
                                self._channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                            self.log.error("Callback method failure: {e}")
                            continue
                    if not auto_ack:
                        self.log.success(f'Message ({msg_id}) from {app_id} received and properly processed -- now dance the funky chicken')
                        self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except (AMQPConnectionError, ConnectionClosedByBroker, StreamLostError) as e:
            log.error(f"Ooooooopsie! I caught a connection error while consuming: {e}")
            raise
        except Exception as e:
            self.log.error(f'Oh lordy lord! I failed consuming ze messaj with: {e}')

    @retry(
        retry=retry_if_exception_type((
            NackError,
            UnroutableError
            )),
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        before_sleep=before_sleep_log(log, WARNING)
           )
    def publish_message(
        self,
        exchange_name: str,
        routing_key: str,
        message: str | bytes | None,
        exchange_type: str,
        queue_name: str,
        auto_declare: bool = True,
        prop: pika.BasicProperties | None = None,
    ) -> None:
        """Publish message to the exchange specifying routing key and properties.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to bind on
        :param bytes body: The message body; empty string if no body
        :param pika.spec.BasicProperties properties: message properties
        :param bool fast_setup:
                - when True, will the method create the specified exchange, queue and bind them together using the routing kye.
                - If False, this method will check if the specified exchange and queue already exist before publishing.

        :raises UnroutableError: raised when a message published in publisher-acknowledgments mode (see `BlockingChannel.confirm_delivery`) is returned via `Basic.Return` followed by `Basic.Ack`.
        :raises NackError: raised when a message published in publisher-acknowledgements mode is Nack'ed by the broker. See `BlockingChannel.confirm_delivery`.
        """
        if not isinstance(message, (str, bytes)):
            raise MrsalAbortedSetup(f'Your message body needs to be string or bytes or serialized dict')
        # connect and use only blocking
        self.setup_blocking_connection()

        if auto_declare:
            if None in (exchange_name, queue_name, exchange_type, routing_key):
                raise TypeError('Make sure that you are passing in all the necessary args for auto_declare')

            self._setup_exchange_and_queue(
                exchange_name=exchange_name,
                queue_name=queue_name,
                exchange_type=exchange_type,
                routing_key=routing_key
                )
        try:
            # Publish the message by serializing it in json dump
            # NOTE! we are not dumping a json anymore here! This allows for more flexibility
            self._channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message, properties=prop)
            self.log.success(f"The message ({message}) is published to the exchange {exchange_name} with the routing key {routing_key}")

        except UnroutableError as e:
            self.log.error(f"Producer could not publish message:{message} to the exchange {exchange_name} with a routing key {routing_key}: {e}", exc_info=True)
            raise
        except NackError as e:
            self.log.error(f"Message NACKed by broker: {e}")
            raise
        except Exception as e:
            self.log.error(f"Unexpected error while publishing message: {e}")

