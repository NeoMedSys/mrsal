import json
import time

import mrsal.config.config as config
import pika
import tests.config as test_config
from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)

mrsal = Mrsal(host=test_config.HOST,
              port=config.RABBITMQ_PORT,
              credentials=config.RABBITMQ_CREDENTIALS,
              virtual_host=config.V_HOST)
mrsal.connect_to_server()

def test_headers_exchange_workflow():

    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange='agreements')
    mrsal.queue_delete(queue='zip_report')
    mrsal.queue_delete(queue='pdf_report')
    # ------------------------------------------

    # Setup exchange
    exch_result: pika.frame.Method = mrsal.setup_exchange(exchange='agreements',
                                                          exchange_type='headers')
    assert exch_result is not None
    # ------------------------------------------

    # Setup queue
    q_result1: pika.frame.Method = mrsal.setup_queue(queue='zip_report')
    assert q_result1 is not None

    # Bind queue to exchange with arguments
    qb_result1: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                              queue='zip_report',
                                                              arguments={'x-match': 'all', 'format': 'zip', 'type': 'report'})
    assert qb_result1 is not None
    # ------------------------------------------

    # Setup queue
    q_result2: pika.frame.Method = mrsal.setup_queue(queue='pdf_report')
    assert q_result2 is not None

    # Bind queue to exchange with arguments
    qb_result2: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                              queue='pdf_report',
                                                              arguments={'x-match': 'any', 'format': 'pdf', 'type': 'log'})
    assert qb_result2 is not None
    # ------------------------------------------

    # Publisher:
    # Message ("uuid1") is published to the exchange with a set of headers
    prop1 = pika.BasicProperties(
        app_id='test_exchange_headers',
        message_id='zip_report',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'format': 'zip', 'type': 'report'})

    message1 = 'uuid1'
    mrsal.publish_message(exchange='agreements',
                          routing_key='',
                          message=json.dumps(message1), prop=prop1
                          )

    # Message ("uuid2") is published to the exchange with a set of headers
    prop2 = pika.BasicProperties(
        app_id='test_exchange_headers',
        message_id='pdf_date',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'format': 'pdf', 'date': '2022'})
    message2 = 'uuid2'
    mrsal.publish_message(exchange='agreements',
                          routing_key='',
                          message=json.dumps(message2), prop=prop2)
    # ------------------------------------------

    time.sleep(1)
    # Confirm messages are routed to respected queues
    result1 = mrsal.setup_queue(queue='zip_report', passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 1

    result2 = mrsal.setup_queue(queue='pdf_report', passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 1
    # ------------------------------------------

    # Start consumer for every queue
    mrsal.start_consumer(
        queue='zip_report',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'zip_report'),
        inactivity_timeout=2,
        requeue=False,
        callback_with_delivery_info=True
    )

    mrsal.start_consumer(
        queue='pdf_report',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'pdf_report'),
        inactivity_timeout=2,
        requeue=False,
        callback_with_delivery_info=True
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result1 = mrsal.setup_queue(queue='zip_report', passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 0

    result2 = mrsal.setup_queue(queue='pdf_report', passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 0


def consumer_callback(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    return True


if __name__ == '__main__':
    test_headers_exchange_workflow()
