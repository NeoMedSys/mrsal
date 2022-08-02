import json
from dataclasses import dataclass
from socket import gaierror
from typing import Callable, Dict, NoReturn, Tuple

import pika
import rabbitamqp.config.config as config
import requests
from rabbitamqp.config.exceptions import RabbitMQConnectionError, RabbitMQDeclareExchangeError
from rabbitamqp.config.logging import get_logger
from requests.auth import HTTPBasicAuth
from retry import retry

log = get_logger(__name__)

@dataclass
class Amqp(object):
    """
    The Amqp creates a layer on top of Pika's core, providing methods to setup a 
    RabbitMQ broker with multiple functionalities.

    Properties:
        :prop str host: Hostname or IP Address to connect to
        :prop int port: TCP port to connect to
        :prop pika.credentials.Credentials credentials: auth credentials
        :prop str virtual_host: RabbitMQ virtual host to use
        :prop bool verbose: If True then more INFO logs will be printed
        :prop int heartbeat: Controls AMQP heartbeat timeout negotiation during connection tuning.
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
    blocked_connection_timeout: int = 300  # 30 sec
    _connection: pika.BlockingConnection = None
    _channel = None

    @retry((pika.exceptions.AMQPConnectionError, TypeError, gaierror), tries=2, delay=5, jitter=(1, 3))
    def setup_connection(self):
        """
        Establish connection to RabbitMQ server specifying connection parameters.
        """
        connection_info = f'host={self.host}, virtual_host={self.virtual_host},port={self.port}, heartbeat={self.heartbeat}'
        if self.verbose:
            log.info(f'Establishing connection to RabbitMQ on ' + connection_info)
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
            log.success(f'Connection established with RabbitMQ on ' + connection_info)
            return self._connection
        except TypeError as err:
            log.error(f'Caught a type error: {err}')
            raise TypeError
        except pika.exceptions.AMQPConnectionError as err:
            log.error(f'Caught a connection error: {err}')
            raise pika.exceptions.AMQPConnectionError
        except gaierror as err:
            log.error(f'Caught a socket error: {err}')
            raise gaierror
        # except Exception as e:
        #     msg = f'No connection to the RabbitMQ server was made on {connection_info}: {str(e)}'
        #     log.error(msg, exc_info=True)
        #     raise RabbitMQConnectionError(msg)

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
            log.info(f'Declaring exchange with: {exchange_declare_info}')
        try:
            exchange_declare_result = self._channel.exchange_declare(exchange=exchange,
                                                                     exchange_type=exchange_type,
                                                                     arguments=arguments,
                                                                     durable=durable,
                                                                     passive=passive,
                                                                     internal=internal,
                                                                     auto_delete=auto_delete)
            log.success(f'Exchange is declared successfully: {exchange_declare_info}, result={exchange_declare_result}')
            return exchange_declare_result
        except TypeError as err:
            log.error(f'Caught a type error: {err}')
            raise TypeError
        except AttributeError as err:
            log.error(f'Caught a attribute error: {err}')
            raise AttributeError
        except pika.exceptions.ConnectionClosedByBroker as err:
            log.error(f'Caught a connection closed by broker error: {err}')
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
            log.info(f'Declaring queue with: {queue_declare_info}')
        try:
            queue_declare_result = self._channel.queue_declare(queue=queue,
                                                               arguments=arguments,
                                                               durable=durable,
                                                               exclusive=exclusive,
                                                               auto_delete=auto_delete,
                                                               passive=passive)
            log.success(f'Queue is declared successfully: {queue_declare_info}, result={queue_declare_result.method}')
            return queue_declare_result
        except pika.exceptions.ChannelClosedByBroker as err:
            log.error(f'Caught ChannelClosedByBroker: {err}')
            raise pika.exceptions.ChannelClosedByBroker(503, str(err))

    def setup_queue_binding(self, exchange: str, queue: str, routing_key: str):
        """Bind the queue to the specified exchange.

        :param str queue: The queue to bind to the exchange
        :param str exchange: The source exchange to bind to
        :param str routing_key: The routing key to bind on

        :returns: Method frame from the Queue.Bind-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
            `spec.Queue.BindOk`

        """
        if self.verbose:
            log.info(f'Binding queue to exchange: queue={queue}, exchange={exchange}, routing_key={routing_key}')
        try:
            bind_result = self._channel.queue_bind(
                exchange=exchange, queue=queue, routing_key=routing_key)
            log.success(f'The queue is bound to exchange successfully: queue={queue}, exchange={exchange}, routing_key={routing_key}, result={bind_result}')
            if self.verbose:
                log.info(f'In such setup a message published to the exchange "{exchange}" \
                            with routing key "{routing_key}" will be routed to queue "{queue}"')
            return bind_result
        except pika.exceptions.ChannelClosedByBroker as err:
            log.error(f'Caught ChannelClosedByBroker: {err}')
            raise pika.exceptions.ChannelClosedByBroker(503, str(err))

    def setup_broker_with_delay_letter(self, exchange: str, routing_key: str, queue: str):
        """
        Setup broker to handle messages with delay time using the delayed-messaging plugin.
        PS! 
            - This plugin is not commercially supported by Pivotal at the moment. 
            - Plugin has known limitations: for more info check here https://github.com/rabbitmq/rabbitmq-delayed-message-exchange#limitations

        Delayed messages:
            We are using the delayed-messaging plugin:  
                - A user can declare an exchange with the type x-delayed-message.
                - Then publish messages with the custom header x-delay expressing in milliseconds a delay time for the message. 
                - The message will be delivered to the respective queues after x-delay milliseconds.

        Workflow scenario with this setup:
            1- PUBLISHER: publish messages to exchange specifying routing-key and x-delay (delay time for the message).
            2- EXCHANGE: send message to queues (bound to the exchange by routing-key) after specified x-delay ms.
            3- QUEUE: receives and stack messages from exchange. 
            4- CONSUMER: start consuming messages from the queue. 
                - These messages can be either 
                    - correctly-acknowledge and deleted from QUEUE or 
                    - negatively-acknowledged with requeue equals to either 
                        - True (The consumer will consume it again from the queue) or 
                        - False (The message will be deleted from queue).

        Params:
            :param str exchange: The exchange name  
                - This exchange is declared with the type x-delayed-message.
                - So when the publisher publish messages with the custom header x-delay 
                    expressing in milliseconds a delay time for the message, 
                    then the message will be delivered to the respective queues after x-delay milliseconds.
            :param str routing_key: The routing key to bind queue to exchange.
            :param str queue: The queue name
        """
        if self.verbose:
            log.info(f'Setting up broker to handle messages with delay time.')

        # Setup main exchange with delay message type
        self.setup_exchange(exchange=exchange,
                            exchange_type=config.DELAY_EXCHANGE_TYPE,
                            arguments=config.DELAY_EXCHANGE_ARGS)

        self.setup_queue(queue=queue)

        # Bind the queue to the main exchange
        self.setup_queue_binding(exchange=exchange,
                                 routing_key=routing_key,
                                 queue=queue)

    def setup_broker_to_handle_dead_messages(self, exchange: str, exchange_type: str, routing_key: str,
                                             dl_exchange: str, dl_exchange_type: str, dl_routing_key: str,
                                             queue: str, dl_queue: str, message_ttl: int = None,
                                             exchange_arguments: Dict[str, str] = None):
        """
        Setup broker to handle dead messages.

        Dead messages are:
            - Some messages become undeliverable or unhandled even when received by the broker. 
            - This can happen when: 
                - The amount of time the message has spent in a queue exceeds the time to live 'TTL' (x-message-ttl). 
                - When a message is negatively-acknowledged by the consumer. 
                - When the queue reaches its capacity.
            - Such a message is called a dead message.


        Workflow scenario with this setup:
            1- PUBLISHER: publish messages to exchange specifying routing-key.
            2- EXCHANGE: send message to queues (bound to the exchange by routing-key).
            3- QUEUE: receives and stack messages from exchange. 
                - A message that has been in the queue for longer than the configured TTL (x-message-ttl) 
                    is said to be expired and will be sent to dead-letters-exchange.
            4- CONSUMER: start consuming not-expired messages from the queue. 
                - These messages can be either 
                    - correctly-acknowledge and deleted from QUEUE or 
                    - negatively-acknowledged and will be send to dead-letters-exchange.
        Params:
            :param str exchange: The Name of main exchange. 
            :param str exchange_type: The type of exchange.
            :param str routing_key: The routing key to bind queue to main exchange.
            :param str dl_exchange: The Name of dead letters exchange.
            :param str dl_exchange_type: The type of dead letters exchange.
            :param str dl_routing_key: The routing key to bind queue to dead letters exchange.
            :param str queue: The main queue name.
            :param str dl_queue: The dead letters queue name.
            :param int message_ttl: Message TTL (time to live) applied to main queue.
            :param Dict[str,str] exchange_arguments: Custom key/value arguments for main exchange.
        """
        if self.verbose:
            log.info(f'Setting up broker to handle dead messages.')

        # Setup dead letters exchange
        self.setup_exchange(exchange=dl_exchange, exchange_type=dl_exchange_type)

        # Setup main exchange
        self.setup_exchange(exchange=exchange,
                            exchange_type=exchange_type,
                            arguments=exchange_arguments)

        # Setup queue with dead letters exchange
        queue_args = {**config.DEAD_LETTER_QUEUE_ARGS,
                      'x-dead-letter-exchange': dl_exchange,
                      'x-dead-letter-routing-key': dl_routing_key}
        # Set message time to live if it presents
        if message_ttl != None:
            queue_args['x-message-ttl'] = message_ttl

        self.setup_queue(queue=queue,
                         arguments=queue_args)

        # Bind the queue to the main exchange
        self.setup_queue_binding(exchange=exchange,
                                 routing_key=routing_key,
                                 queue=queue)

        # Bind the dl_queue to the dl_exchange
        self.setup_queue(queue=dl_queue)
        self.setup_queue_binding(exchange=dl_exchange,
                                 routing_key=dl_routing_key,
                                 queue=dl_queue)

    def setup_dead_and_delay_letters(self, exchange: str, routing_key: str,
                                     dl_exchange: str, dl_exchange_type: str, dl_routing_key: str,
                                     queue: str, dl_queue: str, message_ttl: int = None):
        """
        Setup broker to handle messages which are dead or|and have delayed time.

        1. Dead messages are:
            See doc in :func:`AMQP.setup_dead_letters` 

        2. Delayed messages:
            See doc in :func:`AMQP.setup_delay_letter` 

        Workflow scenario with this setup:
            1- PUBLISHER: publish messages to exchange specifying routing-key and x-delay (delay time for the message).
            2- EXCHANGE: send message to queues (bound to the exchange by routing-key) after specified x-delay ms.
            3- QUEUE: receives and stack messages from exchange. 
                - A message that has been in the queue for longer than the configured TTL (x-message-ttl) 
                    is said to be expired and will be sent to dead-letters-exchange.
            4- CONSUMER: start consuming not-expired messages from the queue. 
                - These messages can be either 
                    - correctly-acknowledge and deleted from QUEUE or 
                    - negatively-acknowledged and will be send to dead-letters-exchange.

        Params:
            :param str exchange: The Name of main exchange. 
                - This exchange is declared with the type x-delayed-message.
                - So when the publisher publish messages with the custom header x-delay 
                    expressing in milliseconds a delay time for the message. 
                - The message will be delivered to the respective queues after x-delay milliseconds.
            :param str routing_key: The routing key to bind on queue to main exchange.
            :param str dl_exchange: The Name of dead letters exchange.
            :param str dl_exchange_type: The type of dead letters exchange.
            :param str dl_routing_key: The routing key to bind on queue to dead letters exchange.
            :param str queue: The main queue name.
            :param str dl_queue: The dead letters queue name.
            :param int message_ttl: Message TTL (time to live) applied to a main queue.
        """
        if self.verbose:
            log.info(f'Setting up broker to handle messages which are dead or|and have delayed time.')

        # Setup dead letters exchange
        self.setup_exchange(exchange=dl_exchange, exchange_type=dl_exchange_type)

        # Setup main exchange with delay message type
        self.setup_exchange(exchange=exchange,
                            exchange_type=config.DELAY_EXCHANGE_TYPE,
                            arguments=config.DELAY_EXCHANGE_ARGS)

        # Setup queue with dead letters exchange
        queue_args = {**config.DEAD_LETTER_QUEUE_ARGS,
                      'x-dead-letter-exchange': dl_exchange,
                      'x-dead-letter-routing-key': dl_routing_key}
        # Set message time to live if it presents
        if message_ttl != None:
            queue_args['x-message-ttl'] = message_ttl

        self.setup_queue(queue=queue,
                         arguments=queue_args)

        # Bind the queue to the main exchange
        self.setup_queue_binding(exchange=exchange,
                                 routing_key=routing_key,
                                 queue=queue)

        # Bind the dl_queue to the dl_exchange
        self.setup_queue(queue=dl_queue)
        self.setup_queue_binding(exchange=dl_exchange,
                                 routing_key=dl_routing_key,
                                 queue=dl_queue)

    # TODO NOT IN USE (remove or use)
    def stop_consuming(self, consumer_tag: str) -> NoReturn:
        self._channel.stop_consuming(consumer_tag=consumer_tag)
        log.info('Consumer is stopped, carry on')

     # TODO NOT IN USE (remove or use)
    def cancel_channel(self, consumer_tag: str) -> NoReturn:
        self._channel.basic_cancel(consumer_tag=consumer_tag)
        log.info('Channel is canceled, carry on')

     # TODO NOT IN USE (remove or use)
    def close_channel(self) -> NoReturn:
        self._channel.close()
        log.info('Channel is closed, carry on')

     # TODO NOT IN USE (remove or use)
    def close_connection(self) -> NoReturn:
        self.close_channel()
        self._connection.close()
        log.info('Connection is closed, carry on')

    def queue_delete(self, queue: str):
        self._channel.queue_delete(queue=queue)

    def exchange_delete(self, exchange: str):
        self._channel.exchange_delete(exchange=exchange)

    def confirm_delivery(self):
        self._channel.confirm_delivery()

    # TODO NOT IN USE:  Test get info from API
    def get_queue_messages_count(self, queue: str):
        url = 'http://localhost:15673/api/queues/bloody_vhost/' + queue
        response = requests.get(url, auth=HTTPBasicAuth('root', 'password'))
        if response.status_code == 200:
            queue_details = response.json()
            if 'messages' in queue_details:
                return queue_details['messages']
            return None
        return None

    # --------------------------------------------------------------
    # --------------------------------------------------------------
    # TODO NOT IN USE: Need to reformat it to publish messages to dead letters exchange after exceeding retries limit
    def consume_messages_with_retries(self, queue: str, callback: Callable, callback_args=None, escape_after=-1,
                                      dead_letters_exchange: str = None, dead_letters_routing_key: str = None,
                                      prop: pika.BasicProperties = None, inactivity_timeout=None):
        log.info(f'Consuming messages: queue= {queue}')

        try:
            for method_frame, properties, body in self._channel.consume(queue=queue, inactivity_timeout=inactivity_timeout):
                consumer_tags = self._channel.consumer_tags
                message = json.loads(body).replace('"', '')
                exchange = method_frame.exchange
                routing_key = method_frame.routing_key
                delivery_tag = method_frame.delivery_tag
                log.info(f'consumer_callback: message: {message}, exchange: {exchange}, routing_key: {routing_key}, delivery_tag: {delivery_tag}, properties: {properties}, consumer_tags: {consumer_tags}')
                is_processed = callback(*callback_args, message)
                log.info(f'is_processed= {is_processed}')
                if is_processed:
                    self._channel.basic_ack(delivery_tag=delivery_tag)
                    # self._channel.basic_nack(delivery_tag=delivery_tag)
                    log.info('Message acknowledged')

                    if method_frame.delivery_tag == escape_after:
                        log.info(
                            f'Break! Max messages to be processed is {escape_after}')
                        break
                else:
                    log.warning(f'Could not process the message= {message}. Process it as dead letter.')
                    is_dead_letter_published = self.publish_dead_letter(message=message, delivery_tag=delivery_tag, dead_letters_exchange=dead_letters_exchange,
                                                                        dead_letters_routing_key=dead_letters_routing_key, prop=prop)
                    if is_dead_letter_published:
                        self._channel.basic_ack(delivery_tag)
                log.info('----------------------------------------------------')
        except FileNotFoundError as e:
            log.info('Connection is closed')
            self._channel.stop_consuming()

    def start_consumer(self, queue: str, callback: Callable, callback_args=None,
                       inactivity_timeout=None, requeue: bool = True):
        """
        Setup consumer:
            1- Consumer start consuming the messages from the queue.
            2- Send the consumed message to callback method to be processed, and then the message can be either:
                - Processed, then correctly-acknowledge and deleted from QUEUE or 
                - Failed to process, then negatively-acknowledged and then will be either
                    - requeued if requeue is True, or
                    - deleted from queue if dead-letters' configuration is not setup and requeue is False or
                    - dead letter if dead-letters' configuration is setup and:
                        - requeue is False or 
                        - requeue is True and requeue attempt fails.

        :param str queue: The queue name to consume
        :param Callable callback: Method where received messages are sent to be processed
        :param Tuple callback_args: Tuple of arguments for callback method
        :param dict arguments: Custom key/value pair arguments for the consumer
        :param float inactivity_timeout: 
            - if a number is given (in seconds), will cause the method to yield (None, None, None) after the
                given period of inactivity.
            - If None is given (default), then the method blocks until the next event arrives.
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.
        """
        log.info(f'Consuming messages: queue= {queue}')

        try:
            for method_frame, properties, body in \
                    self._channel.consume(queue=queue, inactivity_timeout=inactivity_timeout):
                if (method_frame, properties, body) != (None, None, None):
                    consumer_tags = self._channel.consumer_tags
                    consumer_tag = method_frame.consumer_tag
                    message = json.loads(body).replace('"', '')
                    exchange = method_frame.exchange
                    routing_key = method_frame.routing_key
                    delivery_tag = method_frame.delivery_tag
                    redelivered = method_frame.redelivered  # Will be True when the consumer consumes requeued message
                    if self.verbose:
                        log.info(
                            f'Consumed message: message= {message}, method_frame= {method_frame}, redelivered= {redelivered}, exchange= {exchange}, routing_key= {routing_key}, delivery_tag= {delivery_tag}, properties= {properties}, consumer_tags= {consumer_tags}, consumer_tag= {consumer_tag}')
                    is_processed = callback(*callback_args, message)
                    if is_processed:
                        self._channel.basic_ack(delivery_tag=delivery_tag)
                        log.info(f'Message {message} is acknowledged')
                    else:
                        log.warning(f'Could not process the message= {message}. This will be rejected and sent to dead-letters-exchange if it configured or deleted if not.')
                        self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
                        log.warning('Message rejected')
                    log.info('----------------------------------------------------')
                else:
                    log.warning(f'Given period of inactivity {inactivity_timeout} is exceeded. Cancel consumer {consumer_tag}')
                    self._channel.cancel()
        except ValueError as err1:
            log.error(f'ValueError is caught while consuming. Consumer-creation parameters dont match those of the existing queue consumer generator. Cancel consumer {consumer_tag}')
            self._channel.cancel()
        except pika.exceptions.ChannelClosed as err2:
            log.error(f'ChannelClosed is caught while consuming. Channel is closed by broker. Cancel consumer {consumer_tag}')
            self._channel.cancel()

    # --------------------------------------------------------------
    # --------------------------------------------------------------
    @retry((pika.exceptions.UnroutableError), tries=2, delay=5, jitter=(1, 3))
    def publish_message(self, exchange: str, routing_key: str,
                        message: str, properties: pika.BasicProperties):
        """Publish message to the channel with the given exchange, routing key.

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
        try:
            # Publish the message by serializing it in json dump
            self._channel.basic_publish(
                exchange=exchange, routing_key=routing_key, body=json.dumps(
                    message),
                properties=properties)

            log.info(
                f'Message ({message}) is published to the exchange "{exchange}" with a routing key "{routing_key}"')

            # The message will be returned if no one is listening
            return True
        except pika.exceptions.UnroutableError as err1:
            log.error(
                f'Producer could not publish the message ({message}) to the exchange "{exchange}" with a routing key "{routing_key}": {err1}', exc_info=True)
            return False

    # TODO NOT IN USE: maybe we will use it in the method consume_messages_with_retries to publish messages to dead letters exchange after retries limit. (remove or use)
    def publish_dead_letter(self, message: str, delivery_tag: int, dead_letters_exchange: str = None,
                            dead_letters_routing_key: str = None, prop: pika.BasicProperties = None):
        if dead_letters_exchange != None and dead_letters_routing_key != None:
            log.warning(f'Re-route the message= {message} to the exchange= {dead_letters_exchange} with routing_key= {dead_letters_routing_key}')
            try:
                self.publish_message(exchange=dead_letters_exchange,
                                     routing_key=dead_letters_routing_key,
                                     message=json.dumps(message),
                                     properties=prop)
                log.info(f'Dead letter was published: message= {message}, exchange= {dead_letters_exchange}, routing_key= {dead_letters_routing_key}')
                return True
            except pika.exceptions.UnroutableError:
                log.error('Dead letter was returned')
                return False
