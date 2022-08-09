import json
import time

import pika
import rabbitamqp.config.config as config
import tests.config as test_config
from rabbitamqp.config.logging import get_logger
from rabbitamqp.amqp import Amqp

log = get_logger(__name__)

amqp = Amqp(host=test_config.HOST,
            port=config.RABBITMQ_PORT,
            credentials=config.RABBITMQ_CREDENTIALS,
            virtual_host=config.V_HOST)
amqp.connect_to_server()

def test_headers_exchange_workflow():
    EXCHANGE: str = 'agreements'
    EXCHANGE_TYPE: str = 'headers'

    QUEUE_1: str = 'zip_report'
    Q1_ARGS = {'x-match': 'all', 'format': 'zip', 'type': 'report'}

    QUEUE_2: str = 'pdf_report'
    Q2_ARGS = {'x-match': 'any', 'format': 'pdf', 'type': 'log'}

    HEADERS1 = {'format': 'zip', 'type': 'report'}
    HEADERS2 = {'format': 'pdf', 'date': '2022'}
    # ------------------------------------------

    # Delete existing queues and exchanges to use
    amqp.exchange_delete(exchange=EXCHANGE)
    amqp.queue_delete(queue=QUEUE_1)
    amqp.queue_delete(queue=QUEUE_2)
    # ------------------------------------------

    # Setup exchange
    exch_result: pika.frame.Method = amqp.setup_exchange(exchange=EXCHANGE,
                                                         exchange_type=EXCHANGE_TYPE)
    assert exch_result != None
    # ------------------------------------------

    # Setup queue
    q_result1: pika.frame.Method = amqp.setup_queue(queue=QUEUE_1)
    assert q_result1 != None

    # Bind queue to exchange with arguments
    qb_result1: pika.frame.Method = amqp.setup_queue_binding(exchange=EXCHANGE,
                                                             queue=QUEUE_1,
                                                             arguments=Q1_ARGS)
    assert qb_result1 != None
    # ------------------------------------------

    # Setup queue
    q_result2: pika.frame.Method = amqp.setup_queue(queue=QUEUE_2)
    assert q_result2 != None

    # Bind queue to exchange with arguments
    qb_result2: pika.frame.Method = amqp.setup_queue_binding(exchange=EXCHANGE,
                                                             queue=QUEUE_2,
                                                             arguments=Q2_ARGS)
    assert qb_result2 != None
    # ------------------------------------------

    # Publisher:
    # Message ("uuid1") is published to the exchange with a set of headers
    prop1 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers=HEADERS1,
        delivery_mode=pika.DeliveryMode.Persistent)

    message1 = 'uuid1'
    amqp.publish_message(exchange=EXCHANGE,
                         routing_key='',
                         message=json.dumps(message1),
                         properties=prop1)

    # Message ("uuid2") is published to the exchange with a set of headers
    prop2 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers=HEADERS2,
        delivery_mode=pika.DeliveryMode.Persistent)

    message2 = 'uuid2'
    amqp.publish_message(exchange=EXCHANGE,
                         routing_key='',
                         message=json.dumps(message2),
                         properties=prop2)
    # ------------------------------------------

    time.sleep(1)
    # Confirm messages are routed to respected queues
    result1 = amqp.setup_queue(queue=QUEUE_1, passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 1

    result2 = amqp.setup_queue(queue=QUEUE_2, passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 1
    # ------------------------------------------

    # Start consumer for every queue
    amqp.start_consumer(
        queue=QUEUE_1,
        callback=consumer_callback,
        callback_args=(test_config.HOST, QUEUE_1),
        inactivity_timeout=2,
        requeue=False
    )

    amqp.start_consumer(
        queue=QUEUE_2,
        callback=consumer_callback,
        callback_args=(test_config.HOST, QUEUE_2),
        inactivity_timeout=2,
        requeue=False
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result1 = amqp.setup_queue(queue=QUEUE_1, passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 0

    result2 = amqp.setup_queue(queue=QUEUE_2, passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 0


def consumer_callback(host_param: str, queue_param: str, message_param: str):
    return True


if __name__ == '__main__':
    test_headers_exchange_workflow()
