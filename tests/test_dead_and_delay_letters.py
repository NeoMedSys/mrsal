import json
import os
import time

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

    # Establish connection
    amqp.setup_connection()

    amqp.exchange_delete(exchange=test_config.EXCHANGE)
    amqp.exchange_delete(exchange=test_config.DEAD_LETTER_EXCHANGE)
    amqp.exchange_delete(exchange=test_config.DELAY_EXCHANGE)
    amqp.queue_delete(test_config.QUEUE)
    amqp.queue_delete(test_config.DEAD_LETTER_QUEUE)
    amqp.queue_delete(config.RABBITMQ_QUEUE)

    amqp.setup_dead_and_delay_letters(exchange=test_config.EXCHANGE,
                                      routing_key=test_config.ROUTING_KEY,
                                      dl_exchange=test_config.DEAD_LETTER_EXCHANGE,
                                      dl_exchange_type=test_config.EXCHANGE_TYPE,
                                      dl_routing_key=test_config.DEAD_LETTER_ROUTING_KEY,
                                      queue=test_config.QUEUE,
                                      dl_queue=test_config.DEAD_LETTER_QUEUE,
                                      message_ttl=test_config.MESSAGE_TTL)

    dl_amqp = Amqp(host=HOST,
                   port=config.RABBITMQ_DEFAULT_PORT,
                   credentials=config.RABBITMQ_CREDENTIALS,
                   virtual_host=config.V_HOST)
    dl_amqp.setup_connection()

    return HOST, amqp, dl_amqp


def test_dead_letters():
    HOST, amqp, dl_amqp = setup_test()

    x_delay1: int = 2000
    prop1 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'x-delay': x_delay1},
        delivery_mode=pika.DeliveryMode.Persistent)

    message1 = 'uuid1'
    try:
        amqp.publish_message(exchange=test_config.EXCHANGE,
                             routing_key=test_config.ROUTING_KEY,
                             message=json.dumps(message1),
                             properties=prop1)
        log.info(f'Message {message1} was published')
    except pika.exceptions.UnroutableError:
        log.error(f'Message {message1} was returned')

    # Dead message with (x-first-death-reason: rejected) because is negatively acknowledged by the consumer.
    x_delay2: int = 1000
    prop2 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'x-delay': x_delay2},
        delivery_mode=pika.DeliveryMode.Persistent)
    message2 = 'uuid2'
    try:
        amqp.publish_message(exchange=test_config.EXCHANGE,
                             routing_key=test_config.ROUTING_KEY,
                             message=json.dumps(message2),
                             properties=prop2)
        log.info(f'Message {message2} was published')
    except pika.exceptions.UnroutableError:
        log.error(f'Message {message2} was returned')

    # Processing time of this message in the consumer's callback is greater than (test_config.MESSAGE_TTL).
    # That makes all the messages published to the queue (test_config.QUEUE) marked as dead and they will be moved to dl_exchange.
    x_delay3: int = 3000
    prop3 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'x-delay': x_delay3},
        delivery_mode=pika.DeliveryMode.Persistent)
    message3 = 'uuid3'
    try:
        amqp.publish_message(exchange=test_config.EXCHANGE,
                             routing_key=test_config.ROUTING_KEY,
                             message=json.dumps(message3),
                             properties=prop3)
        log.info(f'Message {message3} was published')
    except pika.exceptions.UnroutableError:
        log.error(f'Message {message3} was returned')

    # Dead message with (x-first-death-reason: expired) because:
    # 1. This message is published to the exchange (test_config.EXCHANGE).
    # 2. The queue (test_config.QUEUE) is bound to this exchange with routing key (test_config.ROUTING_KEY).
    # 3. This queue has the argument (x-message-ttl) equals to (test_config.MESSAGE_TTL).
    # 4. The previous message (uuid3) has processing time in the consumer's call_back function greater than (test_config.MESSAGE_TTL).
    # 5. That's why the amount of time the message (uuid4) has spent in the queue exceeded the time to live, (test_config.MESSAGE_TTL).
    # 6. Therefor this message is dead and it moved to the dl_exchange.
    x_delay4: int = 4000
    prop4 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'x-delay': x_delay4},
        delivery_mode=pika.DeliveryMode.Persistent)
    message4 = 'uuid4'
    try:
        amqp.publish_message(exchange=test_config.EXCHANGE,
                             routing_key=test_config.ROUTING_KEY,
                             message=json.dumps(message4),
                             properties=prop4)
        log.info(f'Message {message4} was published')
    except pika.exceptions.UnroutableError:
        log.error(f'Message {message4} was returned')

    log.info('------------------------------------------------------')
    log.info(f'=====Start consuming from {test_config.QUEUE}========')
    log.info('------------------------------------------------------')
    amqp.start_consumer(
        queue=test_config.QUEUE,
        callback=consumer_callback,
        callback_args=(HOST, test_config.QUEUE),
        escape_after=3,
        requeue=False
    )

    log.info('------------------------------------------------------')
    log.info(f'=====Start consuming from {test_config.DEAD_LETTER_QUEUE}========')
    log.info('------------------------------------------------------')
    result = dl_amqp.setup_queue(queue=test_config.DEAD_LETTER_QUEUE)
    message_count = result.method.message_count
    log.info(f'Message count in queue "{test_config.DEAD_LETTER_QUEUE}" before consuming= {message_count}')
    assert message_count == 2

    dl_amqp.start_consumer(
        queue=test_config.DEAD_LETTER_QUEUE,
        callback=consumer_dead_letters_callback,
        callback_args=(HOST, test_config.DEAD_LETTER_QUEUE),
        escape_after=2,
        requeue=False
    )

    result = dl_amqp.setup_queue(queue=test_config.DEAD_LETTER_QUEUE)
    message_count = result.method.message_count
    log.info(f'Message count in queue "{test_config.DEAD_LETTER_QUEUE}" after consuming= {message_count}')
    assert message_count == 0

def consumer_callback(host: str, queue: str, message: str):
    # log.info(f'queue callback: host={host}, queue={queue}, message={message}')
    if message == 'uuid3':
        time.sleep(3)
    return message != 'uuid2'

def consumer_dead_letters_callback(host_param: str, queue_param: str, message_param: str):
    # log.info(f'dl_queue callback: host={host_param}, queue={queue_param}, message={message_param}')
    return True


if __name__ == '__main__':
    test_dead_letters()
