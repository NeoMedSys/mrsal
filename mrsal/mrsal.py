import json
from dataclasses import dataclass
from socket import gaierror
from typing import Callable, Dict, NoReturn, Tuple, Any

import pika
from retry import retry

from mrsal.config.logging import get_logger


@dataclass
class Mrsal:
    """
    Mrsal creates a layer on top of Pika's core, providing methods to setup a 
    RabbitMQ broker with multiple functionalities.

    Properties:
        :prop str host: Hostname or IP Address to connect to
        :prop int port: TCP port to connect to
        :prop pika.credentials.Credentials credentials: auth credentials
        :prop str virtual_host: RabbitMQ virtual host to use
        :prop bool verbose: If True then more INFO logs will be printed
        :prop int heartbeat: Controls RabbitMQ's server heartbeat timeout negotiation during connection tuning.
        :prop int blocked_connection_timeout: blocked_connection_timeout is the timeout, in seconds, 
            for the connection to remain blocked; if the timeout expires, the connection will be torn down
        :prop int prefetch_count: Specifies a prefetch window in terms of whole messages.
    """
    host: str
    port: str
    credentials: Tuple[str, str]
    virtual_host: str
    verbose: bool = False
    prefetch_count: int = 1
    heartbeat: int = 5 * 60 * 60  # 5 hours
    blocked_connection_timeout: int = 300  # sec
    _connection: pika.BlockingConnection = None
    _channel = None
    log = get_logger(__name__)

    @retry((pika.exceptions.AMQPConnectionError, TypeError, gaierror), tries=2, delay=5, jitter=(1, 3))
    def connect_to_server(self):
        """
        Establish connection to RabbitMQ server specifying connection parameters.
        """
        connection_info = f'host={self.host}, virtual_host={self.virtual_host}, port={self.port}, heartbeat={self.heartbeat}'
        if self.verbose:
            self.log.info(f'Establishing connection to RabbitMQ on {connection_info}')
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    virtual_host=self.virtual_host,
                    credentials=pika.PlainCredentials(*self.credentials),
                    heartbeat=self.heartbeat,
                    blocked_connection_timeout=self.blocked_connection_timeout
                ))
            self._channel = self._connection.channel()
            # Note: prefetch is set to 1 here as an example only.
            # In production you will want to test with different prefetch values
            # to find which one provides the best performance and usability for your solution
            self._channel.basic_qos(prefetch_count=self.prefetch_count)
            self.log.success(f'Connection established with RabbitMQ on {connection_info}')
            return self._connection
        except TypeError as err:
            self.log.error(f'Caught a type error: {err}')
            raise TypeError
        except pika.exceptions.AMQPConnectionError as err:
            self.log.error(f'Caught a connection error: {err}')
            raise pika.exceptions.AMQPConnectionError
        except gaierror as err:
            self.log.error(f'Caught a socket error: {err}')
            raise gaierror

    def setup_exchange(self, exchange: str, exchange_type: str, arguments: Dict[str, str] = None,
                       durable=True, passive=False, internal=False, auto_delete=False):
        """This method creates an exchange if it does not already exist, and if
        the exchange exists, verifies that it is of the correct and expected
        class.

        If passive set, the server will reply with Declare-Ok if the exchange
        already exists with the same name, and raise an error if not and if the
        exchange does not already exist, the server MUST raise a channel
        exception with reply code 404 (not found).

        :param str exchange: The exchange name 
        :param str exchange_type: The exchange type to use
        :param bool passive: Perform a declare or just check to see if it exists
        :param bool durable: Survive a reboot of RabbitMQ
        :param bool auto_delete: Remove when no more queues are bound to it
        :param bool internal: Can only be published to by other exchanges
        :param dict arguments: Custom key/value pair arguments for the exchange
        :returns: Method frame from the Exchange.Declare-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
            `spec.Exchange.DeclareOk`
        """
        exchange_declare_info = f'exchange={exchange}, exchange_type={exchange_type}, durable={durable}, passive={passive}, internal={internal}, auto_delete={auto_delete}, arguments={arguments}'
        if self.verbose:
            self.log.info(f'Declaring exchange with: {exchange_declare_info}')
        try:
            exchange_declare_result = self._channel.exchange_declare(exchange=exchange,
                                                                     exchange_type=exchange_type,
                                                                     arguments=arguments,
                                                                     durable=durable,
                                                                     passive=passive,
                                                                     internal=internal,
                                                                     auto_delete=auto_delete)
            self.log.success(f'Exchange is declared successfully: {exchange_declare_info}, result={exchange_declare_result}')
            return exchange_declare_result
        except TypeError as err:
            self.log.error(f'Caught a type error: {err}')
            raise TypeError
        except AttributeError as err:
            self.log.error(f'Caught a attribute error: {err}')
            raise AttributeError
        except pika.exceptions.ConnectionClosedByBroker as err:
            self.log.error(f'Caught a connection closed by broker error: {err}')
            raise pika.exceptions.ConnectionClosedByBroker(503, str(err))

    def setup_queue(self, queue: str, arguments: Dict[str, str] = None, durable: bool = True,
                    exclusive: bool = False, auto_delete: bool = False, passive: bool = False):
        """Declare queue, create if needed. This method creates or checks a
        queue. When creating a new queue the client can specify various
        properties that control the durability of the queue and its contents,
        and the level of sharing for the queue.

        Use an empty string as the queue name for the broker to auto-generate
        one. Retrieve this auto-generated queue name from the returned
        `spec.Queue.DeclareOk` method frame.

        :param str queue: The queue name; if empty string, the broker will
            create a unique queue name
        :param bool passive: Only check to see if the queue exists and raise
          `ChannelClosed` if it doesn't
        :param bool durable: Survive reboots of the broker
        :param bool exclusive: Only allow access by the current connection
        :param bool auto_delete: Delete after consumer cancels or disconnects
        :param dict arguments: Custom key/value arguments for the queue
        :returns: Method frame from the Queue.Declare-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
            `spec.Queue.DeclareOk`

        """
        queue_declare_info = f'queue={queue}, durable={durable}, exclusive={exclusive}, auto_delete={auto_delete}, arguments={arguments}'
        if self.verbose:
            self.log.info(f'Declaring queue with: {queue_declare_info}')
        try:
            queue_declare_result = self._channel.queue_declare(queue=queue,
                                                               arguments=arguments,
                                                               durable=durable,
                                                               exclusive=exclusive,
                                                               auto_delete=auto_delete,
                                                               passive=passive)
            self.log.success(f'Queue is declared successfully: {queue_declare_info}, result={queue_declare_result.method}')
            return queue_declare_result
        except pika.exceptions.ChannelClosedByBroker as err:
            self.log.error(f'Caught ChannelClosedByBroker: {err}')
            raise pika.exceptions.ChannelClosedByBroker(503, str(err))

    def setup_queue_binding(self, exchange: str, queue: str, routing_key: str = None, arguments=None):
        """Bind queue to exchange.

        :param str queue: The queue to bind to the exchange
        :param str exchange: The source exchange to bind to
        :param str routing_key: The routing key to bind on
        :param dict arguments: Custom key/value pair arguments for the binding

        :returns: Method frame from the Queue.Bind-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
            `spec.Queue.BindOk`

        """
        if self.verbose:
            self.log.info(f'Binding queue to exchange: queue={queue}, exchange={exchange}, routing_key={routing_key}')
        try:
            bind_result = self._channel.queue_bind(
                exchange=exchange, queue=queue, routing_key=routing_key, arguments=arguments)
            self.log.success(f'The queue is bound to exchange successfully: queue={queue}, exchange={exchange}, routing_key={routing_key}, result={bind_result}')
            if self.verbose:
                self.log.info(f'In such setup a message published to the exchange "{exchange}" \
                            with routing key "{routing_key}" will be routed to queue "{queue}"')
            return bind_result
        except pika.exceptions.ChannelClosedByBroker as err:
            self.log.error(f'Caught ChannelClosedByBroker: {err}')
            raise pika.exceptions.ChannelClosedByBroker(503, str(err))

    def __stop_consuming(self, consumer_tag: str) -> NoReturn:
        self._channel.stop_consuming(consumer_tag=consumer_tag)
        self.log.info('Consumer is stopped, carry on')

    def __close_channel(self) -> NoReturn:
        self._channel.close()
        self.log.info('Channel is closed, carry on')

    def __close_connection(self) -> NoReturn:
        self.__close_channel()
        self._connection.close()
        self.log.info('Connection is closed, carry on')

    def queue_delete(self, queue: str):
        self._channel.queue_delete(queue=queue)

    def exchange_delete(self, exchange: str):
        self._channel.exchange_delete(exchange=exchange)

    def confirm_delivery(self):
        self._channel.confirm_delivery()

    # --------------------------------------------------------------
    # --------------------------------------------------------------
    # TODO NOT IN USE: Need to reformat it to publish messages to dead letters exchange after exceeding retries limit
    def consume_messages_with_retries(self, queue: str, callback: Callable, callback_args=None, escape_after=-1,
                                      dead_letters_exchange: str = None, dead_letters_routing_key: str = None,
                                      prop: pika.BasicProperties = None, inactivity_timeout=None):
        self.log.info(f'Consuming messages: queue= {queue}')

        try:
            for method_frame, properties, body in self._channel.consume(queue=queue, inactivity_timeout=inactivity_timeout):
                consumer_tags = self._channel.consumer_tags
                # Let the message be in whatever data type it needs to
                message = json.loads(body)
                exchange = method_frame.exchange
                routing_key = method_frame.routing_key
                delivery_tag = method_frame.delivery_tag
                self.log.info(f'consumer_callback info: exchange: {exchange}, routing_key: {routing_key}, delivery_tag: {delivery_tag}, properties: {properties}, consumer_tags: {consumer_tags}')
                is_processed = callback(*callback_args, message)
                self.log.info(f'is_processed= {is_processed}')
                if is_processed:
                    self._channel.basic_ack(delivery_tag=delivery_tag)
                    # self._channel.basic_nack(delivery_tag=delivery_tag)
                    self.log.info('Message acknowledged')

                    if method_frame.delivery_tag == escape_after:
                        self.log.info(
                            f'Break! Max messages to be processed is {escape_after}')
                        break
                else:
                    self.log.warning(f'Could not process the message= {message}. Process it as dead letter.')
                    is_dead_letter_published = self.publish_dead_letter(message=message, delivery_tag=delivery_tag, dead_letters_exchange=dead_letters_exchange,
                                                                        dead_letters_routing_key=dead_letters_routing_key, prop=prop)
                    if is_dead_letter_published:
                        self._channel.basic_ack(delivery_tag)
                self.log.info('----------------------------------------------------')
        except FileNotFoundError as e:
            self.log.error(f'Connection closed with error: {e}')
            self._channel.stop_consuming()

    def start_consumer(
            self, queue: str, callback: Callable, callback_args: Tuple[str, Any] = None,
            exchange: str = None, exchange_type: str = None, routing_key: str = None,
            inactivity_timeout: int = None, requeue: bool = False, fast_setup: bool = False
    ):
        """
        Setup consumer:
            1- Consumer start consuming the messages from the queue.
            2- If `inactivity_timeout` is given (in seconds) the consumer will be canceled when the time of inactivity 
                exceeds inactivity_timeout.
            3- Send the consumed message to callback method to be processed, and then the message can be either:
                - Processed, then correctly-acknowledge and deleted from QUEUE or 
                - Failed to process, negatively-acknowledged and then will be either
                    - `Requeued` if requeue is True
                    - `Dead letter` and deleted from queue if 
                        - requeue is False
                        - requeue is True and requeue attempt fails.

        :param str queue: The queue name to consume
        :param Callable callback: Method where received messages are sent to be processed
        :param Tuple callback_args: Tuple of arguments for callback method
        :param float inactivity_timeout: 
            - if a number is given (in seconds), will cause the method to yield (None, None, None) after the
                given period of inactivity.
            - If None is given (default), then the method blocks until the next event arrives.
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.
        """
        if fast_setup:
            # setting up the necessary connections
            self.setup_exchange(exchange=exchange, exchange_type=exchange_type)
            self.setup_queue(queue=queue)
            self.setup_queue_binding(exchange=exchange, queue=queue, routing_key=routing_key)

        self.log.info(f'Consuming messages: queue= {queue}, requeue= {requeue}, inactivity_timeout= {inactivity_timeout}')

        try:
            for method_frame, properties, body in \
                    self._channel.consume(queue=queue, inactivity_timeout=inactivity_timeout):
                try:
                    if (method_frame, properties, body) != (None, None, None):
                        consumer_tags = self._channel.consumer_tags
                        consumer_tag = method_frame.consumer_tag
                        # let the message be whatever it needs to be
                        if self.verbose:
                            self.log.info(
                                f"""
                                Consumed message:
                                message= {body},
                                method_frame= {method_frame},
                                redelivered= {method_frame.redelivered},
                                exchange= {method_frame.exchange},
                                routing_key= {method_frame.routing_key},
                                delivery_tag= {method_frame.delivery_tag},
                                properties= {properties},
                                consumer_tags= {consumer_tags},
                                consumer_tag= {consumer_tag}
                                """)
                        is_processed = callback(*callback_args, body) if callback_args else callback(body)
                        if is_processed:
                            self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                            self.log.info(f'Message {body} is acknowledged')
                        else:
                            self.log.warning(f'Could not process the message= {body}. This will be rejected and sent to dead-letters-exchange if it configured or deleted if not.')
                            self._channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=requeue)
                            self.log.warning('Message rejected')
                            pass
                        self.log.info(f'[*] keep listening on {queue}.')
                    else:
                        self.log.warning(f'Given period of inactivity {inactivity_timeout} is exceeded. Cancel consumer.')
                        self._channel.cancel()
                except (pika.exceptions.StreamLostError, pika.exceptions.ConnectionClosedByBroker, ValueError, TypeError):
                    self.log.error('I lost the connection with the Mrsal.', exc_info=True)
                    pass
                except KeyboardInterrupt:
                    self.log('Stopping mrsal consumption.')
                    self.__stop_consuming()
                    self.__close_connection()
                    break
        except pika.exceptions.ChannelClosed as err2:
            self.log.error(f'ChannelClosed is caught while consuming. Channel is closed by broker. Cancel consumer. {str(err2)}')
            self._channel.cancel()

    # --------------------------------------------------------------
    # --------------------------------------------------------------
    @retry((pika.exceptions.UnroutableError), tries=2, delay=5, jitter=(1, 3))
    def publish_message(
            self, exchange: str, routing_key: str, message: Any, exchange_type: str = None,
            queue: str = None, content_type: str = 'text/plain', content_encoding: str = 'utf-8',
            delivery_mode: pika.DeliveryMode = pika.DeliveryMode.Persistent, headers: Dict[str, Any] = None, fast_setup: bool = False
    ):
        """Publish message to the exchange specifying routing key and properties.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to bind on
        :param bytes body: The message body; empty string if no body
        :param pika.spec.BasicProperties properties: message properties

        :raises UnroutableError: raised when a message published in
            publisher-acknowledgments mode (see
            `BlockingChannel.confirm_delivery`) is returned via `Basic.Return`
            followed by `Basic.Ack`.
        :raises NackError: raised when a message published in
            publisher-acknowledgements mode is Nack'ed by the broker. See
            `BlockingChannel.confirm_delivery`.

        """
        if fast_setup:
            # setting up the necessary connections
            self.setup_exchange(exchange=exchange, exchange_type=exchange_type)
            self.setup_queue(queue=queue)
            self.setup_queue_binding(exchange=exchange, queue=queue, routing_key=routing_key)

        # configuring the parsing information for the message
        prop = pika.BasicProperties(
            content_type=content_type,
            content_encoding=content_encoding,
            delivery_mode=delivery_mode,
            headers=headers
        )
        try:
            # Publish the message by serializing it in json dump
            self._channel.basic_publish(exchange=exchange,
                                        routing_key=routing_key,
                                        body=json.dumps(message),
                                        properties=prop)
            self.log.info(
                f'Message ({message}) is published to the exchange "{exchange}" with a routing key "{routing_key}"')

            # The message will be returned if no one is listening
            return True
        except pika.exceptions.UnroutableError as err1:
            self.log.error(
                f'Producer could not publish the message ({message}) to the exchange "{exchange}" with a routing key "{routing_key}": {err1}', exc_info=True)
            return False

    # TODO NOT IN USE: maybe we will use it in the method consume_messages_with_retries to publish messages to dead letters exchange after retries limit. (remove or use)
    def publish_dead_letter(self, message: str, delivery_tag: int, dead_letters_exchange: str = None,
                            dead_letters_routing_key: str = None, prop: pika.BasicProperties = None):
        if dead_letters_exchange is not None and dead_letters_routing_key is not None:
            self.log.warning(f'Re-route the message= {message} to the exchange= {dead_letters_exchange} with routing_key= {dead_letters_routing_key}')
            try:
                self.publish_message(exchange=dead_letters_exchange,
                                     routing_key=dead_letters_routing_key,
                                     message=json.dumps(message),
                                     properties=prop)
                self.log.info(f'Dead letter was published: message= {message}, exchange= {dead_letters_exchange}, routing_key= {dead_letters_routing_key}')
                return True
            except pika.exceptions.UnroutableError:
                log.error('Dead letter was returned')
                return False
