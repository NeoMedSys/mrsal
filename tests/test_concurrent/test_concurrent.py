import concurrent.futures
import json
import threading
import time

import pika
from pika.exchange_type import ExchangeType

import mrsal.config.config as config
import tests.config as test_config
from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)

mrsal = Mrsal(host=test_config.HOST,
              port=config.RABBITMQ_PORT,
              credentials=config.RABBITMQ_CREDENTIALS,
              virtual_host=config.V_HOST)
mrsal.connect_to_server()

APP_ID = "test_exchange_direct"
EXCHANGE = "agreements"
EXCHANGE_TYPE = ExchangeType.direct
QUEUE_MADRID = 'agreements_madrid_queue'
QUEUE_BERLIN = 'agreements_berlin_queue'

def consumer_func(index: int):
    mrsal_arg = Mrsal(host=test_config.HOST,
                      port=config.RABBITMQ_PORT,
                      credentials=config.RABBITMQ_CREDENTIALS,
                      virtual_host=config.V_HOST)

    mrsal_arg.connect_to_server()
    log.info(f"--> start consumer: index={index}")
    mrsal_arg.start_consumer(
        queue=QUEUE_MADRID,
        callback=consumer_callback,
        callback_args=(test_config.HOST, QUEUE_MADRID),
        inactivity_timeout=1,
        requeue=False,
    )
    mrsal_arg.close_connection()

def test_direct_exchange_workflow():

    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange=EXCHANGE)
    mrsal.queue_delete(queue=QUEUE_BERLIN)
    mrsal.queue_delete(queue=QUEUE_MADRID)
    # ------------------------------------------

    # Setup exchange
    exch_result: pika.frame.Method = mrsal.setup_exchange(exchange=EXCHANGE,
                                                          exchange_type=EXCHANGE_TYPE)
    assert exch_result != None
    # ------------------------------------------

    # Setup queue for berlin agreements
    q_result1: pika.frame.Method = mrsal.setup_queue(queue=QUEUE_BERLIN)
    assert q_result1 != None

    # Bind queue to exchange with binding key
    qb_result1: pika.frame.Method = mrsal.setup_queue_binding(exchange=EXCHANGE,
                                                              routing_key='berlin agreements',
                                                              queue=QUEUE_BERLIN)
    assert qb_result1 != None
    # ------------------------------------------

    # Setup queue for madrid agreements
    q_result2: pika.frame.Method = mrsal.setup_queue(queue=QUEUE_MADRID)
    assert q_result2 != None

    # Bind queue to exchange with binding key
    qb_result2: pika.frame.Method = mrsal.setup_queue_binding(exchange=EXCHANGE,
                                                              routing_key='madrid agreements',
                                                              queue=QUEUE_MADRID)
    assert qb_result2 != None
    # ------------------------------------------

    # Publisher:
    # Publish 10 messages to QUEUE_MADRID
    num_msgs = 10
    for msg_index in range(num_msgs):
        prop1 = pika.BasicProperties(
            app_id=APP_ID,
            message_id='madrid_uuid_' + str(msg_index),
            content_type=test_config.CONTENT_TYPE,
            content_encoding=test_config.CONTENT_ENCODING,
            delivery_mode=pika.DeliveryMode.Persistent,
            headers=None)
        message2 = 'uuid_' + str(msg_index)
        mrsal.publish_message(exchange=EXCHANGE,
                              routing_key='madrid agreements',
                              message=json.dumps(message2), prop=prop1)

    prop2 = pika.BasicProperties(
        app_id=APP_ID,
        message_id='berlin_uuid',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
    # Message ("uuid1") is published to the exchange and it's routed to queue1
    message1 = 'uuid1'
    mrsal.publish_message(exchange=EXCHANGE,
                          routing_key='berlin agreements',
                          message=json.dumps(message1), prop=prop2)
    # ------------------------------------------

    time.sleep(1)
    # Confirm messages are routed to respected queues
    result1 = mrsal.setup_queue(queue=QUEUE_BERLIN, passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 1

    result2 = mrsal.setup_queue(queue=QUEUE_MADRID, passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 10
    # ------------------------------------------

    mrsal.start_consumer(
        queue=QUEUE_BERLIN,
        callback=consumer_callback_with_delivery_info,
        callback_args=(test_config.HOST, QUEUE_BERLIN),
        inactivity_timeout=1,
        requeue=False,
        callback_with_delivery_info=True
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(consumer_func, range(5))

    # Confirm messages are consumed
    result1 = mrsal.setup_queue(queue=QUEUE_BERLIN, passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 0

    result2 = mrsal.setup_queue(queue=QUEUE_MADRID, passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 0

    mrsal.close_connection()

def consumer_callback_with_delivery_info(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    time.sleep(5)
    return True

def consumer_callback(host_param: str, queue_param: str, message_param: str):
    time.sleep(5)
    return True


if __name__ == '__main__':
    test_direct_exchange_workflow()
