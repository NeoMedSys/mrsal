import json
import os

import pika
import rabbitamqp.config.config as config
from rabbitamqp.config.logging import get_logger
from rabbitamqp.src.amqp import Amqp

import tests.config as test_config
import tests.helper as helper

log = get_logger(__name__)

host = os.environ.get('RABBITMQ_HOST', 'localhost')
port = config.RABBITMQ_DEFAULT_PORT
credentials = config.RABBITMQ_CREDENTIALS
exchange: str = config.RABBITMQ_EXCHANGE
exchange_type = config.RABBITMQ_EXCHANGE_TYPE
routing_key = config.RABBITMQ_BIND_ROUTING_KEY
queue = config.RABBITMQ_QUEUE
dead_letter_routing_key = config.RABBITMQ_DEAD_LETTER_ROUTING_KEY
dead_letter_queue = config.RABBITMQ_DEAD_LETTER_QUEUE

amqp: Amqp = helper.setup_amqp(host, port, credentials, exchange, exchange_type,
                               queue, routing_key, dead_letter_queue, dead_letter_routing_key)

amqp.confirm_delivery()
log.info('===========================================')

def test_producer():
    prop = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'key': 'value'},
        delivery_mode=pika.DeliveryMode.Persistent)

    for index in range(5):
        message = test_config.TEST_MESSAGE + str(index)
        try:
            amqp.basic_publish(exchange=exchange,
                               routing_key=routing_key,
                               message=json.dumps(message),
                               properties=prop)
            log.info('Message was published')
        except pika.exceptions.UnroutableError:
            log.error('Message was returned')


if __name__ == '__main__':
    test_producer()
