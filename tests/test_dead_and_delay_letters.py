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

    # Publisher:
    #   Message ("uuid1") is published with x-delay=2000
    #   Message ("uuid2") is published with x-delay=1000
    #   Message ("uuid3") is published with x-delay=3000
    #   Message ("uuid4") is published with x-delay=4000
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
    # --------------------------------------------------------------
    # --------------------------------------------------------------
    log.info('--------------------------------------------------------')
    log.info(f'===== Start consuming from {test_config.QUEUE} ========')
    log.info('--------------------------------------------------------')
    # Consumer from main queue
    #   Message ("uuid2"): Consumed first because its delivered from exchange to the queue
    #     after x-delay=1000ms which is the shortest time.
    #       - This message is rejected by consumer's callback.
    #       - Therefor it will be negatively-acknowledged by consumer.
    #       - Then it will be forwarded to dead-letters-exchange (x-first-death-reason: rejected).
    #   Message ("uuid1"): Consumed at second place because its x-delay = 2000 ms.
    #       - This message is positively-acknowledged by consumer.
    #       - Then it will be deleted from queue.
    #   Message ("uuid3"): Consumed at third place because its x-delay = 3000 ms.
    #       - This message has processing time in the consumer's callback equal to 3s
    #           which is greater that TTL=2s.
    #       - After processing will be positively-acknowledged by consumer.
    #       - Then it will be deleted from queue.
    #   Message ("uuid4"): Consumed at fourth place because its x-delay = 4000 ms.
    #       - This message will be forwarded to dead-letters-exchange
    #           because it spent in the queue more than TTL=2s waiting "uuid3" to be processed
    #           (x-first-death-reason: expired).
    amqp.start_consumer(
        queue=test_config.QUEUE,
        callback=consumer_callback,
        callback_args=(HOST, test_config.QUEUE),
        inactivity_timeout=6,
        requeue=False
    )
    # --------------------------------------------------------------
    # --------------------------------------------------------------
    log.info('------------------------------------------------------')
    log.info(f'===== Start consuming from {test_config.DEAD_LETTER_QUEUE} ========')
    log.info('------------------------------------------------------')
    # Consumer from dead letters queue
    #   Message ("uuid2"):
    #       - This message is positively-acknowledged by consumer.
    #       - Then it will be deleted from dl-queue.
    #   Message ("uuid4"):
    #       - This message is positively-acknowledged by consumer.
    #       - Then it will be deleted from dl-queue.
    result = dl_amqp.setup_queue(queue=test_config.DEAD_LETTER_QUEUE)
    message_count = result.method.message_count
    log.info(f'Message count in queue "{test_config.DEAD_LETTER_QUEUE}" before consuming= {message_count}')
    assert message_count == 2

    dl_amqp.start_consumer(
        queue=test_config.DEAD_LETTER_QUEUE,
        callback=consumer_dead_letters_callback,
        callback_args=(HOST, test_config.DEAD_LETTER_QUEUE),
        inactivity_timeout=3,
        requeue=False
    )

    result = dl_amqp.setup_queue(queue=test_config.DEAD_LETTER_QUEUE)
    message_count = result.method.message_count
    log.info(f'Message count in queue "{test_config.DEAD_LETTER_QUEUE}" after consuming= {message_count}')
    assert message_count == 0

def consumer_callback(host: str, queue: str, message: str):
    if message == 'uuid3':
        time.sleep(3)
    return message != 'uuid2'

def consumer_dead_letters_callback(host_param: str, queue_param: str, message_param: str):
    return True


if __name__ == '__main__':
    test_dead_letters()
