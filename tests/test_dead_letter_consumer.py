import os

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
log.info('===========================================')
def test_dead_letter_consumer():
    host = os.environ.get('RABBITMQ_HOST', 'localhost')
    amqp.consume_messages(
        queue=dead_letter_queue,
        callback=consumer_dead_letters_callback,
        callback_args=(host, queue),
        escape_after=-1,
        close_after_consuming=True
    )

def consumer_dead_letters_callback(host_param: str, queue_param: str, message_param: str):
    log.info(f'consume callback: host={host_param}, queue={queue_param}, message={message_param}')
    return True


if __name__ == '__main__':
    test_dead_letter_consumer()
