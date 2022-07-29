import json
from turtle import delay
from retry import retry
from typing import Callable, Dict, NoReturn, Tuple
from dataclasses import dataclass

import pika
import rabbitamqp.config.config as config
import requests
from rabbitamqp.config.exceptions import RabbitMQConnectionError
from rabbitamqp.config.logging import get_logger
from requests.auth import HTTPBasicAuth

log = get_logger(__name__)

@dataclass
class Amqp(object):
    host: str
    port: str
    credentials: Tuple[str, str]
    virtual_host: str
    prefetch_count: int = 1
    heartbeat: int = 60 * 60  # 60 min
    blocked_connection_timeout: int = 300  # 30 sec
    _connection: pika.BlockingConnection = None
    _channel = None

    # def __init__(self, host: str, port: int, credentials: Tuple[str, str],
    #              virtual_host: str = config.V_HOST, prefetch_count: int = 1,
    #              heartbeat: int = 600, blocked_connection_timeout: int = 300):
    #     self.host = host
    #     self.port = port
    #     self.credentials = credentials
    #     self.virtual_host = virtual_host
    #     self.prefetch_count = prefetch_count
    #     self.heartbeat = heartbeat
    #     self.blocked_connection_timeout = blocked_connection_timeout
    #     self._connection = None
    #     self._channel = None

# --------------------------------------------------------------
# CONNECTION
# --------------------------------------------------------------
    # @retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
    @retry(RabbitMQConnectionError, delay=2, tries=2)
    def establish_connection(self):
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
            log.success(f'Connection established with RabbitMQ on {self.host}')
            return self._connection
        except Exception as e:
            log.error(
                f'No connection to the RabbitMQ server on {self.host} was made: {str(e)}', exc_info=True)
            raise RabbitMQConnectionError(
                f'No connection to the RabbitMQ server on {self.host} was made: {str(e)}')

    def setup_exchange(self, exchange: str, exchange_type: str, arguments: Dict[str, str] = None, durable=True, passive=False, auto_delete=False):
        self._channel.exchange_declare(exchange=exchange,
                                       exchange_type=exchange_type,
                                       arguments=arguments,
                                       durable=durable,
                                       passive=passive,
                                       auto_delete=auto_delete)
        log.info(
            f'Exchange ({exchange}) is declared in RabbitMQ to post messages to by "Producers"')

    def setup_queue(self, queue: str, arguments: Dict[str, str] = None, durable=True, exclusive=False, auto_delete=False):
        result = self._channel.queue_declare(queue=queue,
                                             arguments=arguments,
                                             durable=durable,
                                             exclusive=exclusive,
                                             auto_delete=auto_delete)
        log.info(f'result.method: {result.method}')
        return result

    def setup_queue_binding(self, exchange: str, queue: str, routing_key: str):
        self._channel.queue_bind(
            exchange=exchange, queue=queue, routing_key=routing_key)
        log.info(
            f'The queue "{queue}" is bound to the exchange "{exchange}" using routing key "{routing_key}"')
        log.info(
            f'In such a setup a message published to the exchange "{exchange}" with a routing key "{routing_key}" will be routed to queue "{queue}"')

    def setup_delay_letter(self, exchange: str, routing_key: str, queue: str):
        """
        A user can declare an exchange with the type x-delayed-message 
        and then publish messages with the custom header x-delay 
        expressing in milliseconds a delay time for the message. 
        The message will be delivered to the respective queues after x-delay milliseconds.
        """

        # Setup main exchange with delay message type
        self.setup_exchange(exchange=exchange,
                            exchange_type=config.DELAY_EXCHANGE_TYPE,
                            arguments=config.DELAY_EXCHANGE_ARGS)

        self.setup_queue(queue=queue)

        # Bind the queue to the main exchange
        self.setup_queue_binding(exchange=exchange,
                                 routing_key=routing_key,
                                 queue=queue)

    def setup_dead_letters(self, exchange: str, exchange_type: str, routing_key: str,
                           dl_exchange: str, dl_exchange_type: str, dl_routing_key: str,
                           queue: str, dl_queue: str, message_ttl: int = None, exchange_arguments: Dict[str, str] = None):
        """
        Some messages become undeliverable or unhandled even when received by the broker. 
        This can happen when: 
            - The amount of time the message has spent in a queue exceeds the time to live, TTL. 
            - When a message is negatively acknowledged by the consumer. 
            - When the queue reaches its capacity.
        Such a message is called a dead message.
        """
        # message_ttl: A message that has been in the queue for longer than the configured TTL is said to be dead.

        # Setup dead letters exchange
        self.setup_exchange(exchange=dl_exchange, exchange_type=dl_exchange_type)

        # Setup main exchange with delay message type
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
        self._channel.queue_bind(exchange=exchange,
                                 routing_key=routing_key,
                                 queue=queue)

        # Bind the dl_queue to the dl_exchange
        self.setup_queue(queue=dl_queue)
        self._channel.queue_bind(exchange=dl_exchange,
                                 routing_key=dl_routing_key,
                                 queue=dl_queue)

    def setup_dead_and_delay_letters(self, exchange: str, routing_key: str,
                                     dl_exchange: str, dl_exchange_type: str, dl_routing_key: str,
                                     queue: str, dl_queue: str, message_ttl: int = None):
        """
        Process dead and delayed messages.

        1. Dead messages:
            Some messages become undeliverable or unhandled even when received by the broker. 
            This can happen when: 
                - The amount of time the message has spent in a queue exceeds the time to live, TTL. 
                - When a message is negatively acknowledged by the consumer. 
                - When the queue reaches its capacity.
            Such a message is called a dead message.

        2. Delayed messages
            We are using the delayed-messaging feature:  
                - A user can declare an exchange with the type x-delayed-message.
                - Then publish messages with the custom header x-delay expressing in milliseconds a delay time for the message. 
                - The message will be delivered to the respective queues after x-delay milliseconds.

        PUBLISHER ---publish messages with x-delay--->  
            EXCHANGE ---send message to bound queues after x-delay ms--->
                QUEUE ---receives messages--->
                    - Send not-expired messages to CONSUMER to process them
                        - Process the message correctly acknowledge and delete it from QUEUE.
                        or
                        - Send negative-acknowledged messages to dead letters exchange.
                    or
                    - Send expired messages to dead letters exchange



        :param str exchange: The Name of main exchange. 
            - This exchange is declared with the type x-delayed-message.
            - So when the publisher publish messages with the custom header x-delay expressing in milliseconds a delay time for the message. 
            - The message will be delivered to the respective queues after x-delay milliseconds.
        :param str routing_key: The routing key to bind on queue to main exchange.
        :param str dl_exchange: The Name of dead letters exchange.
        :param str dl_exchange_type: The type of dead letters exchange.
        :param str dl_routing_key: The routing key to bind on queue to dead letters exchange.
        :param str queue: The main queue name.
        :param str dl_queue: The dead letters queue name.
        :param int message_ttl: Message TTL (time to live) applied to a main queue.
        """
        # message_ttl: A message that has been in the queue for longer than the configured TTL is said to be dead.

        self.setup_dead_letters(exchange=exchange, exchange_type=config.DELAY_EXCHANGE_TYPE, routing_key=routing_key,
                                dl_exchange=dl_exchange, dl_exchange_type=dl_exchange_type,
                                dl_routing_key=dl_routing_key, queue=queue, dl_queue=dl_queue, message_ttl=message_ttl,
                                exchange_arguments=config.DELAY_EXCHANGE_ARGS)

    def stop_consuming(self, consumer_tag: str) -> NoReturn:
        self._channel.stop_consuming(consumer_tag=consumer_tag)
        log.info('Consumer is stopped, carry on')

    def cancel_channel(self, consumer_tag: str) -> NoReturn:
        self._channel.basic_cancel(consumer_tag=consumer_tag)
        log.info('Channel is canceled, carry on')

    def close_channel(self) -> NoReturn:
        self._channel.close()
        log.info('Channel is closed, carry on')

    def close_connection(self) -> NoReturn:
        self.close_channel()
        self._connection.close()
        log.info('Connection is closed, carry on')

    def get_channel(self):
        return self._channel

    def queue_delete(self, queue: str):
        self._channel.queue_delete(queue=queue)

    def exchange_delete(self, exchange: str):
        self._channel.exchange_delete(exchange=exchange)

    def get_connection(self):
        return self._connection

    def confirm_delivery(self):
        self._channel.confirm_delivery()

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
    # CONSUMER
    # --------------------------------------------------------------
    def consume_messages(self, queue: str, callback: Callable, callback_args=None, escape_after=-1,
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

    def consume_messages_aux(self, queue: str, callback: Callable, callback_args=None,
                             escape_after=-1, inactivity_timeout=None):
        log.info(f'Consuming messages: queue= {queue}')

        try:
            for method_frame, properties, body in \
                    self._channel.consume(queue=queue, inactivity_timeout=inactivity_timeout):
                consumer_tags = self._channel.consumer_tags
                message = json.loads(body).replace('"', '')
                exchange = method_frame.exchange
                routing_key = method_frame.routing_key
                delivery_tag = method_frame.delivery_tag
                redelivered = method_frame.redelivered
                log.info(f'consumer_callback: message: {message}, exchange: {exchange}, routing_key: {routing_key}, delivery_tag: {delivery_tag}, properties: {properties}, consumer_tags: {consumer_tags}')
                is_processed = callback(*callback_args, message)
                log.info(f'is_processed= {is_processed}')
                # properties.headers['x-retry-count'] = 4
                if is_processed:
                    self._channel.basic_ack(delivery_tag=delivery_tag)
                    log.info('Message acknowledged')

                    if method_frame.delivery_tag == escape_after:
                        log.info(
                            f'Break! Max messages to be processed is {escape_after}')
                        break
                else:
                    log.warning(f'Could not process the message= {message}. Process it as dead letter.')
                    self._channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                    # self._channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                    log.info('Message rejected')
                log.info('----------------------------------------------------')
        except FileNotFoundError as e:
            log.info('Connection is closed')
            self._channel.stop_consuming()

    # --------------------------------------------------------------
    # PRODUCER
    # --------------------------------------------------------------
    def basic_publish(self, exchange: str, routing_key: str, message: str, properties: pika.BasicProperties):
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
        except pika.exceptions.UnroutableError as e:
            log.error(
                f'Producer could not publish the message ({message}) to the exchange "{exchange}" with a routing key "{routing_key}": {e}', exc_info=True)
            return False

    def publish_dead_letter(self, message: str, delivery_tag: int, dead_letters_exchange: str = None,
                            dead_letters_routing_key: str = None, prop: pika.BasicProperties = None):
        if dead_letters_exchange != None and dead_letters_routing_key != None:
            log.warning(f'Re-route the message= {message} to the exchange= {dead_letters_exchange} with routing_key= {dead_letters_routing_key}')
            try:
                self.basic_publish(exchange=dead_letters_exchange,
                                   routing_key=dead_letters_routing_key,
                                   message=json.dumps(message),
                                   properties=prop)
                log.info(f'Dead letter was published: message= {message}, exchange= {dead_letters_exchange}, routing_key= {dead_letters_routing_key}')
                return True
            except pika.exceptions.UnroutableError:
                log.error('Dead letter was returned')
                return False
