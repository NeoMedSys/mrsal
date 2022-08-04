import json
import os

import pika
import rabbitamqp.config.config as config
from rabbitamqp.config.logging import get_logger
from rabbitamqp.src.amqp import Amqp

import tests.config as test_config

log = get_logger(__name__)

def setup_test():
    HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
    amqp = Amqp(host=HOST,
                port=config.RABBITMQ_DEFAULT_PORT,
                credentials=config.RABBITMQ_CREDENTIALS,
                virtual_host=config.V_HOST)

    amqp.setup_connection()

    amqp.exchange_delete(exchange=test_config.EXCHANGE)
    amqp.exchange_delete(exchange=test_config.DEAD_LETTER_EXCHANGE)
    amqp.exchange_delete(exchange=test_config.DELAY_EXCHANGE)
    amqp.queue_delete(test_config.QUEUE)
    amqp.queue_delete(test_config.DEAD_LETTER_QUEUE)
    amqp.queue_delete(config.RABBITMQ_QUEUE)

    amqp.setup_broker_with_delay_letter(exchange=test_config.DELAY_EXCHANGE,
                                        routing_key=test_config.DELAY_ROUTING_KEY,
                                        queue=test_config.QUEUE)
    return HOST, amqp

def test_delay_letter():
    HOST, amqp = setup_test()

    # Publisher:
    #   Message ("uuid1") is published with x-delay=3000
    #   Message ("uuid2") is published with x-delay=1000
    x_delay1: int = 3000
    message1 = 'uuid1'
    prop1 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'x-delay': x_delay1},
        delivery_mode=pika.DeliveryMode.Persistent)
    try:
        amqp.publish_message(exchange=test_config.DELAY_EXCHANGE,
                             routing_key=test_config.DELAY_ROUTING_KEY,
                             message=json.dumps(message1),
                             properties=prop1)
        log.info(f'Message {message1} was published')
    except pika.exceptions.UnroutableError:
        log.error(f'Message {message1} was returned')

    x_delay2: int = 1000
    message2 = 'uuid2'
    prop2 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'x-delay': x_delay2},
        delivery_mode=pika.DeliveryMode.Persistent)
    try:
        amqp.publish_message(exchange=test_config.DELAY_EXCHANGE,
                             routing_key=test_config.DELAY_ROUTING_KEY,
                             message=json.dumps(message2),
                             properties=prop2)
        log.info(f'Message {message2} was published')
    except pika.exceptions.UnroutableError:
        log.error(f'Message {message2} was returned')
    # --------------------------------------------------------------
    # --------------------------------------------------------------
    log.info('--------------------------------------------------------')
    log.info(f'===== Start consuming from {test_config.QUEUE} ========')
    log.info('--------------------------------------------------------')
    # Consumer from main queue
    #   Message ("uuid2"): Consumed first because its delivered from exchange to the queue
    #    after x-delay=1000ms which is the shortest time.
    #   Message ("uuid1"): Consumed at second place because its x-delay = 3000 ms.
    amqp.start_consumer(
        queue=test_config.QUEUE,
        callback=consumer_callback,
        callback_args=(HOST, test_config.QUEUE),
        inactivity_timeout=3,
        requeue=False
    )

def consumer_callback(host: str, queue: str, message: str):
    return True


if __name__ == '__main__':
    test_delay_letter()
