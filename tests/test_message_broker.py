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
virtual_host = config.V_HOST

# =================================================
amqp_producer: Amqp = helper.setup_amqp(host, port, credentials, virtual_host, exchange, exchange_type,
                                        queue, routing_key, dead_letter_queue, dead_letter_routing_key)
amqp_consumer: Amqp = helper.setup_amqp(host, port, credentials, virtual_host, exchange, exchange_type,
                                        queue, routing_key, dead_letter_queue, dead_letter_routing_key)
amqp_consumer_dead_letters: Amqp = helper.setup_amqp(host, port, credentials, virtual_host, exchange, exchange_type,
                                                     queue, routing_key, dead_letter_queue, dead_letter_routing_key)
# =================================================
def test_broker():

    prop = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'key': 'value'},
        delivery_mode=pika.DeliveryMode.Persistent)

    num_messages_to_publish = 5
    num_messages_to_dead = 1
    for index in range(num_messages_to_publish):
        message = test_config.TEST_MESSAGE + str(index)
        try:
            amqp_producer.basic_publish(exchange=exchange,
                                        routing_key=routing_key,
                                        message=json.dumps(message),
                                        properties=prop)
            log.info('Message was published')
        except pika.exceptions.UnroutableError:
            log.error('Message was returned')

    result = amqp_consumer.queue_declare(queue=queue)
    message_count = result.method.message_count
    log.info(f'Message count in queue "{queue}" before consuming= {message_count}')
    assert message_count == num_messages_to_publish

    amqp_consumer.consume_messages(
        queue=queue,
        callback=consumer_callback,
        callback_args=(host, queue),
        escape_after=5,
        dead_letters_exchange=exchange,
        dead_letters_routing_key=dead_letter_routing_key
    )

    result = amqp_consumer.queue_declare(queue=queue)
    message_count = result.method.message_count
    log.info(f'Message count in queue "{queue}" after consuming= {message_count}')
    assert message_count == 0

    result = amqp_consumer_dead_letters.queue_declare(queue=dead_letter_queue)
    message_count = result.method.message_count
    log.info(f'Message count in queue "{dead_letter_queue}" before consuming= {message_count}')
    assert message_count == num_messages_to_dead
    amqp_consumer_dead_letters.consume_messages(
        queue=dead_letter_queue,
        callback=consumer_dead_letters_callback,
        callback_args=(host, queue),
        escape_after=1
    )

    result = amqp_consumer_dead_letters.queue_declare(queue=dead_letter_queue)
    message_count = result.method.message_count
    log.info(f'Message count in queue "{dead_letter_queue}" after consuming= {message_count}')
    assert message_count == 0


def consumer_dead_letters_callback(host_param: str, queue_param: str, message_param: str):
    log.info(f'consume callback: host={host_param}, queue={queue_param}, message={message_param}')
    return True

def consumer_callback(host: str, queue: str, message: str):
    log.info(f'consume callback: host={host}, queue={queue}, message={message}')
    return message != test_config.TEST_MESSAGE + str(test_config.TEST_MESSAGE_INDEX)


if __name__ == '__main__':
    test_broker()
