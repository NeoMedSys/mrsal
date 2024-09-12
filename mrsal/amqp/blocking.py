import pika
from pika import SSLOptions
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker, StreamLostError, ConnectionClosedByBroker, UnroutableError
from pika.adapters.asyncio_connection import AsyncioConnection
from typing import Callable, Any, Optional, Type
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_sleep_log
from pydantic import ValidationError
from pydantic.dataclasses import dataclass
from neolibrary.monitoring.logger import NeoLogger

from mrsal.mrsal import Mrsal
from pydantic.deprecated.tools import json

log = NeoLogger(__name__, log_days=10)

@dataclass
class MrsalBlockingAMQP(Mrsal):
    @retry(
        retry=retry_if_exception_type(
            AMQPConnectionError,
            ChannelClosedByBroker,
            ConnectionClosedByBroker,
            StreamLostError,
            ),
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        before_sleep=before_sleep_log(log, log.warning)
           )
    def setup_connection(self, context: dict[str, str] | None = None):
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
        if self.ssl:
            self.log.info("Setting up TLS connection")
            context = self._ssl_setup()
        ssl_options = pika.SSLOptions(context, self.host) if context else None
        credentials = pika.PlainCredentials(*self.credentials)
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    ssl_options=ssl_options,
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
        except (pika.exceptions.AMQPConnectionError, Exception) as err:
            self.log.error(f"I tried to connect with the RabbitMQ server but failed with: {err}")


    def start_consumer(self,
                       queue_name: str,
                       callback: Callable | None = None,
                       callback_args: dict[str, str | int | float | bool] | None = None,
                       auto_ack: bool = True,
                       inactivity_timeout: int = 5,
                       auto_declare: bool = True,
                       exchange_name: str | None = None,
                       exchange_type: str | None = None,
                       routing_key: str | None = None,
                       payload_model: Type | None = None
                       ):
        """
        Start the consumer using blocking setup.
        :param queue: The queue to consume from.
        :param auto_ack: If True, messages are automatically acknowledged.
        :param inactivity_timeout: Timeout for inactivity in the consumer loop.
        :param callback: The callback function to process messages.
        :param callback_args: Optional arguments to pass to the callback.
        :param payload_model: Optional pydantic BaseModel class that specifies expected payload arg types
        """
        if auto_declare:
            if None in (exchange_name, queue_name, exchange_type, routing_key):
                raise TypeError('Make sure that you are passing in all the necessary args for auto_declare, yia bish')

            self._setup_exchange_and_queue(
                    exchange_name=exchange_name,
                    queue_name=queue_name,
                    exchange_type=exchange_type,
                    routing_key=routing_key
                    )
        try:
            for method_frame, properties, body in self._channel.consume(
                    queue=queue_name, auto_ack=auto_ack, inactivity_timeout=inactivity_timeout
                    ):
                if method_frame:
                    app_id = properties.app_id if properties else None
                    msg_id = properties.msg_id if properties else None

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
                        self.log.success(f'I successfully received a message from: {app_id} with messageID: {msg_id}')
                    
                    if payload_model:
                        try:
                            self.validate_payload(body, payload_model)
                        except (ValidationError, json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
                            self.log.error(f"Oh lordy lord, payload validation failed for your specific model requirements: {e}")
                            continue
                    if callback:
                        if callback_args:
                            callback(*callback_args, method_frame, properties, body)
                        else:
                            callback( method_frame, properties, body)
                else:
                    # continue consuming
                    continue
        except Exception as e:
            self.log.error(f'Oh lordy lord! I failed consuming ze messaj with: {e}')

    def publish_message(
        self,
        exchange_name: str,
        routing_key: str,
        message: Any,
        exchange_type: str,
        queue_name: str,
        auto_declare: bool = True,
        prop: pika.BasicProperties | None = None,
    ):
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


@dataclass
class MrsalAsyncAMQP(Mrsal):
    @retry(
        retry=retry_if_exception_type(
            AMQPConnectionError,
            ChannelClosedByBroker,
            ConnectionClosedByBroker,
            StreamLostError,
            ),
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        before_sleep=before_sleep_log(log, log.warning)
           )
    async def setup_aync_connection(self, context: dict[str, str] | None = None):
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
        if self.ssl:
            self.log.info("Setting up TLS connection")
            context = self._ssl_setup()
        ssl_options = SSLOptions(context, self.host) if context else None
        credentials = pika.PlainCredentials(*self.credentials)

        try:
            await AsyncioConnection.create_connection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    ssl_options=ssl_options,
                    virtual_host=self.virtual_host,
                    credentials=credentials,
                    heartbeat=self.heartbeat,
                ),
                on_done=self.on_connection_open,
                on_open_error_callback=self.on_connection_error
            )
        except Exception as e:
            self.log.error(f"Oh lordy lord I failed connecting to the Rabbit with: {e}")

        self.log.success(f"Boom! Connection established with RabbitMQ on {connection_info}")


    async def start_consumer(self,
                             queue_name: str,
                             callback: Callable | None = None,
                             callback_args: dict[str, str | int | float | bool] | None = None,
                             auto_ack: bool = True,
                             inactivity_timeout: int = 5,
                             auto_declare: bool = True,
                             exchange_name: str | None = None,
                             exchange_type: str | None = None,
                             routing_key: str | None = None,
                             payload_model: Type | None = None
                             ):
        """
        Start the consumer using blocking setup.
        :param queue: The queue to consume from.
        :param auto_ack: If True, messages are automatically acknowledged.
        :param inactivity_timeout: Timeout for inactivity in the consumer loop.
        :param callback: The callback function to process messages.
        :param callback_args: Optional arguments to pass to the callback.
        """
        if auto_declare:
            if None in (exchange_name, queue_name, exchange_type, routing_key):
                raise TypeError('Make sure that you are passing in all the necessary args for auto_declare')
            await self._setup_exchange_and_queue(
                    exchange_name=exchange_name,
                    queue_name=queue_name,
                    exchange_type=exchange_type,
                    routing_key=routing_key
                    )

        try:
            async for method_frame, properties, body in self._channel.consume(
                    queue=queue_name, auto_ack=auto_ack, inactivity_timeout=inactivity_timeout
                    ):
                if method_frame:
                    app_id = properties.app_id if properties else None
                    msg_id = properties.msg_id if properties else None

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
                        self.log.success(f'I successfully received a message from: {app_id} with messageID: {msg_id}')
                    
                    if payload_model:
                        try:
                            self.validate_payload(body, payload_model)
                        except (ValidationError, json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
                            self.log.error(f"Oh lordy lord, payload validation failed for your specific model requirements: {e}")
                            continue

                    if callback:
                        if callback_args:
                            await callback(*callback_args, method_frame, properties, body)
                        else:

                            await callback( method_frame, properties, body)
                else:
                    # continue consuming
                    continue
        except Exception as e:
            self.log.error(f'Oh lordy lord! I failed consuming ze messaj with: {e}')

    async def publish_message(
        self,
        exchange_name: str,
        routing_key: str,
        message: Any,
        exchange_type: str,
        queue_name: str,
        auto_declare: bool = True,
        prop: pika.BasicProperties | None = None,
    ):
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
        if auto_declare:
            if None in (exchange_name, queue_name, exchange_type, routing_key):
                raise TypeError('Make sure that you are passing in all the necessary args for auto_declare')
            await self._setup_exchange_and_queue(
                    exchange_name=exchange_name,
                    queue_name=queue_name,
                    exchange_type=exchange_type,
                    routing_key=routing_key
                    )
        try:
            # Publish the message by serializing it in json dump
            # NOTE! we are not dumping a json anymore here! This allows for more flexibility
            self._channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message, properties=prop)
            self.log.success(f"The message ({message}) is published to the exchange {exchange_name} with the following routing key {routing_key}")

        except UnroutableError as e:
            self.log.error(f"Producer could not publish message:{message} to the exchange {exchange_name} with a routing key {routing_key}: {e}", exc_info=True)
