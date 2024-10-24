import asyncio
from mrsal.basemodels import MrsalProtocol
import pika
import json
from mrsal.exceptions import MrsalAbortedSetup, MrsalNoAsyncioLoopError
from logging import WARNING
from pika.exceptions import (
        AMQPConnectionError,
        ChannelClosedByBroker,
        StreamLostError,
        ConnectionClosedByBroker,
        NackError,
        UnroutableError
        )
from aio_pika import connect_robust, Channel as AioChannel
from typing import Callable, Type
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_sleep_log
from pydantic import ValidationError
from pydantic.dataclasses import dataclass
from neolibrary.monitoring.logger import NeoLogger

from mrsal.superclass import Mrsal
from mrsal import config

log = NeoLogger(__name__, rotate_days=config.LOG_DAYS)

@dataclass
class MrsalBlockingAMQP(Mrsal):
    """
    :param int blocked_connection_timeout: blocked_connection_timeout
        is the timeout, in seconds,
        for the connection to remain blocked; if the timeout expires,
            the connection will be torn down during connection tuning.
    """
    blocked_connection_timeout: int = 60  # sec


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
                    ssl_options=self.get_ssl_context(async_conn=False),
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
                     payload_model: Type | None = None,
                     requeue: bool = True
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

            if not self.auto_declare_ok:
                raise MrsalAbortedSetup('Auto declaration for the connection setup failed and is aborted')

        self.log.info(f"Straigh out of the swamps -- consumer boi listening on queue: {queue_name} to the exchange {exchange_name}. Waiting for messages...")

        try:
            for method_frame, properties, body in self._channel.consume(
                                queue=queue_name, auto_ack=auto_ack, inactivity_timeout=inactivity_timeout):
                if method_frame:
                    if properties:
                        app_id = properties.app_id if hasattr(properties, 'app_id') else 'no AppID given'
                        msg_id = properties.message_id if hasattr(properties, 'message_id') else 'no msgID given'

                    if self.verbose:
                        self.log.info(
                                f"""
                                Message received with:
                                - Method Frame: {method_frame}
                                - Redelivery: {method_frame.redelivered}
                                - Exchange: {method_frame.exchange}
                                - Routing Key: {method_frame.routing_key}
                                - Delivery Tag: {method_frame.delivery_tag}
                                - Properties: {properties}
                                - Requeue: {requeue}
                                - Auto Ack: {auto_ack}
                                """
                                )
                    if auto_ack:
                        self.log.info(f'I successfully received a message with AutoAck from: {app_id} with messageID: {msg_id}')
                    
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
                                callback(method_frame, properties, body)
                        except Exception as e:
                            if not auto_ack:
                                self._channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=requeue)
                            self.log.error(f"Callback method failure: {e}")
                            self.log.error(f"Oh lordy lord message {msg_id} from {app_id} failed while running callback")
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
            self.log.success(f"The message ({message!r}) is published to the exchange {exchange_name} with the routing key {routing_key}")

        except UnroutableError as e:
            self.log.error(f"Producer could not publish message:{message!r} to the exchange {exchange_name} with a routing key {routing_key}: {e}", exc_info=True)
            raise
        except NackError as e:
            self.log.error(f"Message NACKed by broker: {e}")
            raise
        except Exception as e:
            self.log.error(f"Unexpected error while publishing message: {e}")

    @retry(
        retry=retry_if_exception_type((
            NackError,
            UnroutableError
            )),
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        before_sleep=before_sleep_log(log, WARNING)
           )
    def publish_messages(
        self,
        mrsal_protocol_collection: dict[str, dict[str, str | bytes]],
        prop: pika.BasicProperties | None = None,
    ) -> None:
        """Publish message to the exchange specifying routing key and properties.

        mrsal_protocol_collection :  dict[str, dict[str, str | bytes]] 
            This is a collection of the protcols needed for publishing to multiple exhanges at once

            expected collection: {
                inbound_app_1: {message: bytes | str, routing_key: str, queue_name: str, exchange_type: str, exchange_name: str},
                inbound_app_2: {message: bytes | str, routing_key: str, queue_name: str, exchange_type: str, exchange_name: str},
                .,
                .
            }

        :raises UnroutableError: raised when a message published in publisher-acknowledgments mode (see `BlockingChannel.confirm_delivery`) is returned via `Basic.Return` followed by `Basic.Ack`.
        :raises NackError: raised when a message published in publisher-acknowledgements mode is Nack'ed by the broker. See `BlockingChannel.confirm_delivery`.
        """
        
        for inbound_app_id, mrsal_protocol in mrsal_protocol_collection.items():
            protocol = MrsalProtocol(**mrsal_protocol)

            if not isinstance(protocol.message, (str, bytes)):
                raise MrsalAbortedSetup(f'Your message body needs to be string or bytes or serialized dict')
                # connect and use only blocking
            self.setup_blocking_connection()
            self._setup_exchange_and_queue(
                exchange_name=protocol.exchange_name,
                queue_name=protocol.queue_name,
                exchange_type=protocol.exchange_type,
                routing_key=protocol.routing_key
                )
            try:
                # Publish the message by serializing it in json dump
                # NOTE! we are not dumping a json anymore here! This allows for more flexibility
                self._channel.basic_publish(
                        exchange=protocol.exchange_name,
                        routing_key=protocol.routing_key,
                        body=protocol.message,
                        properties=prop
                        )
                self.log.success(f"The message for inbound app {inbound_app_id} with message -- ({protocol.message!r}) is published to the exchange {protocol.exchange_name} with the routing key {protocol.routing_key}. Oh baby baby")

            except UnroutableError as e:
                self.log.error(f"Producer could not publish message:{protocol.message!r} to the exchange {protocol.exchange_name} with a routing key {protocol.routing_key}: {e}", exc_info=True)
                raise
            except NackError as e:
                self.log.error(f"Message NACKed by broker: {e}")
                raise
            except Exception as e:
                self.log.error(f"Unexpected error while publishing message: {e}")


class MrsalAsyncAMQP(Mrsal):
    """Handles asynchronous connection with RabbitMQ using aio-pika."""
    async def setup_async_connection(self):
        """Setup an asynchronous connection to RabbitMQ using aio-pika."""
        self.log.info(f"Establishing async connection to RabbitMQ on {self.host}:{self.port}")
        try:
            self._connection = await connect_robust(
                host=self.host,
                port=self.port,
                login=self.credentials[0],
                password=self.credentials[1],
                virtualhost=self.virtual_host,
                ssl=self.ssl,
                ssl_context=self.get_ssl_context(),
                heartbeat=self.heartbeat
            )
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=self.prefetch_count)
            self.log.info("Async connection established successfully.")
        except (AMQPConnectionError, StreamLostError, ChannelClosedByBroker, ConnectionClosedByBroker) as e:
            self.log.error(f"Error establishing async connection: {e}", exc_info=True)
            raise
        except Exception as e:
            self.log.error(f'Oh my lordy lord! I caugth an unexpected exception while trying to connect: {e}', exc_info=True)

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
    async def start_consumer(
            self, 
            queue_name: str,
            callback: Callable | None = None,
            callback_args: dict[str, str | int | float | bool] | None = None,
            auto_ack: bool = False,
            auto_declare: bool = True,
            exchange_name: str | None = None,
            exchange_type: str | None = None,
            routing_key: str | None = None,
            payload_model: Type | None = None,
            requeue: bool = True
            ):
        """Start the async consumer with the provided setup."""
        # Check if there's a connection; if not, create one
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            raise MrsalNoAsyncioLoopError(f'Young grasshopper! You forget to add asyncio.run(mrsal.start_consumer(...))')
        if not self._connection:
            await self.setup_async_connection()

        
        self._channel: AioChannel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self.prefetch_count)

        if auto_declare:
            if None in (exchange_name, queue_name, exchange_type, routing_key):
                raise TypeError('Make sure that you are passing in all the necessary args for auto_declare')

            queue = await self._async_setup_exchange_and_queue(
                    exchange_name=exchange_name,
                    queue_name=queue_name,
                    exchange_type=exchange_type,
                    routing_key=routing_key
                    )

            if not self.auto_declare_ok:
                if self._connection:
                    await self._connection.close()
                raise MrsalAbortedSetup('Auto declaration failed during setup.')

        self.log.info(f"Straight out of the swamps -- Consumer boi listening on queue: {queue_name}, exchange: {exchange_name}")

        # async with queue.iterator() as queue_iter:
        async for message in queue.iterator():
            if message is None:
                continue

            # Extract message metadata
            app_id = message.app_id if hasattr(message, 'app_id') else 'NoAppID'
            msg_id = message.app_id if hasattr(message, 'message_id') else 'NoMsgID'

            # add this so it is in line with Pikas awkawrdly old ways
            properties = config.AioPikaAttributes(app_id=app_id, message_id=msg_id)

            if self.verbose:
                self.log.info(f"""
                            Message received with:
                            - Redelivery: {message.redelivered}
                            - Exchange: {message.exchange}
                            - Routing Key: {message.routing_key}
                            - Delivery Tag: {message.delivery_tag}
                            - Requeue: {requeue}
                            - Auto Ack: {auto_ack}
                            """)

            if auto_ack:
                await message.ack()
                self.log.info(f'I successfully received a message from: {app_id} with messageID: {msg_id}')

            if payload_model:
                try:
                    self.validate_payload(message.body, payload_model)
                except (ValidationError, json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
                    self.log.error(f"Payload validation failed: {e}", exc_info=True)
                    if not auto_ack:
                        await message.reject(requeue=requeue)
                    continue

            if callback:
                try:
                    if callback_args:
                        await callback(*callback_args, message, properties, message.body)
                    else:
                        await callback(message, properties, message.body)
                except Exception as e:
                    self.log.error(f"Splæt! Error processing message with callback: {e}", exc_info=True)
                    if not auto_ack:
                        await message.reject(requeue=requeue)
                    continue

            if not auto_ack:
                await message.ack()
                self.log.success(f'Young grasshopper! Message ({msg_id}) from {app_id} received and properly processed.')

