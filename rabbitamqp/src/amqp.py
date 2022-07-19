import json
from typing import Callable, NoReturn, Tuple

import pika
from rabbitamqp.config.exceptions import RabbitMQConnectionError
from rabbitamqp.config.logging import get_logger

log = get_logger(__name__)


class Amqp(object):
    def __init__(self, host: str, port: int, credentials: Tuple[str, str]):
        self.host = host
        self.port = port
        self.credentials = credentials
        self.connection = None
        self.channel = None

# --------------------------------------------------------------
# CONNECTION
# --------------------------------------------------------------
    def establish_connection(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    credentials=pika.PlainCredentials(*self.credentials)
                ))
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=1)
            log.success(f'Connection established with RabbitMQ on {self.host}')
            return self.connection
        except Exception as e:
            log.error(
                f'No connection to the RabbitMQ server on {self.host} was made: {str(e)}', exc_info=True)
            raise RabbitMQConnectionError(
                f'No connection to the RabbitMQ server on {self.host} was made: {str(e)}')

    def exchange_declare(self, exchange: str, exchange_type: str):
        # exchange_type can be either direct, topic, headers or fanout
        # We will use direct: The routing algorithm behind a direct exchange is simple -
        # a message goes to the queues whose binding key exactly matches the routing key of the message
        self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
        log.info(
            f'Exchange ({exchange}) is declared in RabbitMQ to post messages to by "Producers"')

    def queue_declare(self, queue: str, durable=True, exclusive=False, auto_delete=False):
        result = self.channel.queue_declare(
            queue=queue, durable=True, exclusive=False, auto_delete=False)
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
    # --------------------------------------------------------------
    # CONSUMER
    # --------------------------------------------------------------

    def consume_messages(self, queue: str, callback: Callable, callback_args=None, escape_after=-1,
                         dead_letters_exchange: str = None, dead_letters_routing_key: str = None,
                         prop: pika.BasicProperties = None, close_after_consuming: bool = False):
        log.info(f'Consuming messages: queue= {queue}')
        result = self.queue_declare(queue=queue)
        message_count = result.method.message_count
        log.info(f'message_count = {message_count}')
        if close_after_consuming and message_count == 0:
            self.channel.stop_consuming()
            self.close_connection()
        else:
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
                        log.info('Message acknowledged')
                        self.channel.basic_ack(delivery_tag)

                        if method_frame.delivery_tag == escape_after:
                            log.info(
                                f'Break! Max messages to be processed is {escape_after}')
                            break
                    else:
                        log.warning(f'Could not process the message= {message}')
                        if dead_letters_exchange != None and dead_letters_routing_key != None:
                            log.warning(f'Re-route the message= {message} to the exchange= {exchange} with routing_key= {routing_key}')
                            try:
                                self.basic_publish(exchange=dead_letters_exchange,
                                                   routing_key=dead_letters_routing_key,
                                                   message=json.dumps(message),
                                                   properties=prop)
                                log.info(f'Not processed message was published: message= {message}, dead_letters_exchange= {dead_letters_exchange}, dead_letters_routing_key= {dead_letters_routing_key}')
                                self.channel.basic_ack(delivery_tag)
                                continue
                            except pika.exceptions.UnroutableError:
                                log.error('Not processed message was returned')
                    if close_after_consuming:
                        result = self.queue_declare(queue=queue)
                        message_count = result.method.message_count
                        log.info(f'message_count = {message_count}')
                        if message_count == 0:
                            break
                    log.info('----------------------------------------------------')
            except FileNotFoundError as e:
                log.info('Connection is closed')
                self.close_connection()
            if close_after_consuming:
                self.channel.stop_consuming()
                self.close_connection()

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
