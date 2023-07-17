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

def test_direct_exchange_workflow():

    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange='agreements')
    mrsal.queue_delete(queue='agreements_berlin_queue')
    mrsal.queue_delete(queue='agreements_madrid_queue')
    # ------------------------------------------

    # Setup exchange
    exch_result: pika.frame.Method = mrsal.setup_exchange(exchange='agreements',
                                                          exchange_type='direct')
    assert exch_result is not None
    # ------------------------------------------

    # Setup queue for berlin agreements
    q_result1: pika.frame.Method = mrsal.setup_queue(queue='agreements_berlin_queue')
    assert q_result1 is not None

    # Bind queue to exchange with binding key
    qb_result1: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                              routing_key='berlin agreements',
                                                              queue='agreements_berlin_queue')
    assert qb_result1 is not None
    # ------------------------------------------

    # Setup queue for madrid agreements
    q_result2: pika.frame.Method = mrsal.setup_queue(queue='agreements_madrid_queue')
    assert q_result2 is not None

    # Bind queue to exchange with binding key
    qb_result2: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                              routing_key='madrid agreements',
                                                              queue='agreements_madrid_queue')
    assert qb_result2 is not None
    # ------------------------------------------

    # Publisher:
    prop1 = pika.BasicProperties(
        app_id='test_exchange_direct',
        message_id='madrid_uuid',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)

    # Message ("uuid2") is published to the exchange and it's routed to queue2
    message2 = 'uuid2'
    mrsal.publish_message(exchange='agreements',
                          routing_key='madrid agreements',
                          message=json.dumps(message2), prop=prop1)

    prop2 = pika.BasicProperties(
        app_id='test_exchange_direct',
        message_id='berlin_uuid',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
    # Message ("uuid1") is published to the exchange and it's routed to queue1
    message1 = 'uuid1'
    mrsal.publish_message(exchange='agreements',
                          routing_key='berlin agreements',
                          message=json.dumps(message1), prop=prop2)
    # ------------------------------------------

    time.sleep(1)
    # Confirm messages are routed to respected queues
    result1 = mrsal.setup_queue(queue='agreements_berlin_queue', passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 1

    result2 = mrsal.setup_queue(queue='agreements_madrid_queue', passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 1
    # ------------------------------------------

    # Start consumer for every queue
    mrsal.start_consumer(
        queue='agreements_berlin_queue',
        callback=consumer_callback_with_delivery_info,
        callback_args=(test_config.HOST, 'agreements_berlin_queue'),
        inactivity_timeout=1,
        requeue=False,
        callback_with_delivery_info=True
    )

    mrsal.start_consumer(
        queue='agreements_madrid_queue',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'agreements_madrid_queue'),
        inactivity_timeout=1,
        requeue=False,
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result1 = mrsal.setup_queue(queue='agreements_berlin_queue', passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 0

    result2 = mrsal.setup_queue(queue='agreements_madrid_queue', passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 0

def consumer_callback_with_delivery_info(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    return True

def consumer_callback(host_param: str, queue_param: str, message_param: str):
    return True


if __name__ == '__main__':
    test_direct_exchange_workflow()
