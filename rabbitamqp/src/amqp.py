import json
from typing import Callable, NoReturn, Tuple

import pika
import rabbitamqp.config.config as config
import requests
from rabbitamqp.config.exceptions import RabbitMQConnectionError
from rabbitamqp.config.logging import get_logger
from requests.auth import HTTPBasicAuth

log = get_logger(__name__)

class Amqp(object):
    def __init__(self, host: str, port: int, credentials: Tuple[str, str],
                 virtual_host: str = config.V_HOST,
                 heartbeat: int = 600, blocked_connection_timeout: int = 300):
        self.host = host
        self.port = port
        self.credentials = credentials
        self.virtual_host = virtual_host
        self.heartbeat = heartbeat
        self.blocked_connection_timeout = blocked_connection_timeout
        self.connection = None
        self.channel = None

# --------------------------------------------------------------
# CONNECTION
# --------------------------------------------------------------
    def establish_connection(self, ):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    virtual_host=self.virtual_host,
                    credentials=pika.PlainCredentials(*self.credentials),
                    heartbeat=self.heartbeat,
                    blocked_connection_timeout=self.blocked_connection_timeout
                ))
            self.channel = self.connection.channel()
            # Note: prefetch is set to 1 here as an example only.
            # In production you will want to test with different prefetch values
            # to find which one provides the best performance and usability for your solution
            self.channel.basic_qos(prefetch_count=1)
            log.success(f'Connection established with RabbitMQ on {self.host}')
            return self.connection
        except Exception as e:
            log.error(
                f'No connection to the RabbitMQ server on {self.host} was made: {str(e)}', exc_info=True)
            raise RabbitMQConnectionError(
                f'No connection to the RabbitMQ server on {self.host} was made: {str(e)}')

    def exchange_declare(self, exchange: str, exchange_type: str, durable=True, passive=False, auto_delete=False):
        # exchange_type can be either direct, topic, headers or fanout
        # We will use direct: The routing algorithm behind a direct exchange is simple -
        # a message goes to the queues whose binding key exactly matches the routing key of the message
        self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=durable, passive=passive, auto_delete=auto_delete)
        log.info(
            f'Exchange ({exchange}) is declared in RabbitMQ to post messages to by "Producers"')

    def queue_declare(self, queue: str, durable=True, exclusive=False, auto_delete=False):
        result = self.channel.queue_declare(
            queue=queue, durable=durable, exclusive=exclusive, auto_delete=auto_delete)
        log.info(f'result.method: {result.method}')
        return result

    def queue_bind(self, exchange: str, queue: str, routing_key: str):
        self.channel.queue_bind(
            exchange=exchange, queue=queue, routing_key=routing_key)
        log.info(
            f'The queue "{queue}" is bound to the exchange "{exchange}" using routing key "{routing_key}"')
        log.info(
            f'In such a setup a message published to the exchange "{exchange}" with a routing key "{routing_key}" will be routed to queue "{queue}"')

    def stop_consuming(self, consumer_tag: str) -> NoReturn:
        self.channel.stop_consuming(consumer_tag=consumer_tag)
        log.info('Consumer is stopped, carry on')

    def cancel_channel(self, consumer_tag: str) -> NoReturn:
        self.channel.basic_cancel(consumer_tag=consumer_tag)
        log.info('Channel is canceled, carry on')

    def close_channel(self) -> NoReturn:
        self.channel.close()
        log.info('Channel is closed, carry on')

    def close_connection(self) -> NoReturn:
        self.close_channel()
        self.connection.close()
        log.info('Connection is closed, carry on')

    def get_channel(self):
        return self.channel

    def get_connection(self):
        return self.connection

    def confirm_delivery(self):
        self.channel.confirm_delivery()

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
                         prop: pika.BasicProperties = None):
        log.info(f'Consuming messages: queue= {queue}')

        try:
            for method_frame, properties, body in self.channel.consume(queue):
                consumer_tags = self.channel.consumer_tags
                message = json.loads(body).replace('"', '')
                exchange = method_frame.exchange
                routing_key = method_frame.routing_key
                delivery_tag = method_frame.delivery_tag
                log.info(f'consumer_callback: message: {message}, exchange: {exchange}, routing_key: {routing_key}, delivery_tag: {delivery_tag}, properties: {properties}, consumer_tags: {consumer_tags}')
                is_processed = callback(*callback_args, message)
                log.info(f'is_processed= {is_processed}')
                if is_processed:
                    self.channel.basic_ack(delivery_tag)
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
                        self.channel.basic_ack(delivery_tag)
                log.info('----------------------------------------------------')
        except FileNotFoundError as e:
            log.info('Connection is closed')
            self.channel.stop_consuming()

    # --------------------------------------------------------------
    # PRODUCER
    # --------------------------------------------------------------
    def basic_publish(self, exchange: str, routing_key: str, message: str, properties: pika.BasicProperties):
        try:
            # Publish the message by serializing it in json dump
            self.channel.basic_publish(
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

    def publish_dead_letter(self, message: str, delivery_tag: int, dead_letters_exchange: str = None, dead_letters_routing_key: str = None,
                            prop: pika.BasicProperties = None):
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
