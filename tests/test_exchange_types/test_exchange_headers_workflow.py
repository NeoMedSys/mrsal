import json
import time

import pika
import rabbitamqp.config.config as config
import tests.config as test_config
from rabbitamqp.amqp import Amqp
from rabbitamqp.config.logging import get_logger

log = get_logger(__name__)

amqp = Amqp(host=test_config.HOST,
            port=config.RABBITMQ_PORT,
            credentials=config.RABBITMQ_CREDENTIALS,
            virtual_host=config.V_HOST)
amqp.connect_to_server()

def test_headers_exchange_workflow():

    # Delete existing queues and exchanges to use
    amqp.exchange_delete(exchange='agreements')
    amqp.queue_delete(queue='zip_report')
    amqp.queue_delete(queue='pdf_report')
    # ------------------------------------------

    # Setup exchange
    exch_result: pika.frame.Method = amqp.setup_exchange(exchange='agreements',
                                                         exchange_type='headers')
    assert exch_result != None
    # ------------------------------------------

    # Setup queue
    q_result1: pika.frame.Method = amqp.setup_queue(queue='zip_report')
    assert q_result1 != None

    # Bind queue to exchange with arguments
    qb_result1: pika.frame.Method = amqp.setup_queue_binding(exchange='agreements',
                                                             queue='zip_report',
                                                             arguments={'x-match': 'all', 'format': 'zip', 'type': 'report'})
    assert qb_result1 != None
    # ------------------------------------------

    # Setup queue
    q_result2: pika.frame.Method = amqp.setup_queue(queue='pdf_report')
    assert q_result2 != None

    # Bind queue to exchange with arguments
    qb_result2: pika.frame.Method = amqp.setup_queue_binding(exchange='agreements',
                                                             queue='pdf_report',
                                                             arguments={'x-match': 'any', 'format': 'pdf', 'type': 'log'})
    assert qb_result2 != None
    # ------------------------------------------

    # Publisher:
    # Message ("uuid1") is published to the exchange with a set of headers
    prop1 = pika.BasicProperties(
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        headers={'format': 'zip', 'type': 'report'},
        delivery_mode=pika.DeliveryMode.Persistent)

    message1 = 'uuid1'
    amqp.publish_message(exchange='agreements',
                         routing_key='',
                         message=json.dumps(message1),
                         properties=prop1)

    # Message ("uuid2") is published to the exchange with a set of headers
    prop2 = pika.BasicProperties(
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        headers={'format': 'pdf', 'date': '2022'},
        delivery_mode=pika.DeliveryMode.Persistent)

    message2 = 'uuid2'
    amqp.publish_message(exchange='agreements',
                         routing_key='',
                         message=json.dumps(message2),
                         properties=prop2)
    # ------------------------------------------

    time.sleep(1)
    # Confirm messages are routed to respected queues
    result1 = amqp.setup_queue(queue='zip_report', passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 1

    result2 = amqp.setup_queue(queue='pdf_report', passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 1
    # ------------------------------------------

    # Start consumer for every queue
    amqp.start_consumer(
        queue='zip_report',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'zip_report'),
        inactivity_timeout=2,
        requeue=False
    )

    amqp.start_consumer(
        queue='pdf_report',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'pdf_report'),
        inactivity_timeout=2,
        requeue=False
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result1 = amqp.setup_queue(queue='zip_report', passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 0

    result2 = amqp.setup_queue(queue='pdf_report', passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 0


def consumer_callback(host_param: str, queue_param: str, message_param: str):
    return True


if __name__ == '__main__':
    test_headers_exchange_workflow()
