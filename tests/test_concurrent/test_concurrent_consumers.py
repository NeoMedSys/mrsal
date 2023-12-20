import json
import time

import pika
from pika.exchange_type import ExchangeType

import mrsal.config.config as config
import tests.config as test_config
from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)

mrsal = Mrsal(host=test_config.HOST, port=config.RABBITMQ_PORT, credentials=config.RABBITMQ_CREDENTIALS, virtual_host=config.V_HOST)
mrsal.connect_to_server()

APP_ID = "TEST_CONCURRENT_CONSUMERS"
EXCHANGE = "CLINIC"
EXCHANGE_TYPE = ExchangeType.direct
QUEUE_EMERGENCY = "EMERGENCY"
NUM_THREADS = 3
NUM_MESSAGES = 3
INACTIVITY_TIMEOUT = 3
ROUTING_KEY = "PROCESS FOR EMERGENCY"
MESSAGE_ID = "HOSPITAL_EMERGENCY_MRI_"


def test_concurrent_consumer():
    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange=EXCHANGE)
    mrsal.queue_delete(queue=QUEUE_EMERGENCY)
    # ------------------------------------------
    # Setup exchange
    exch_result: pika.frame.Method = mrsal.setup_exchange(exchange=EXCHANGE, exchange_type=EXCHANGE_TYPE)
    assert exch_result is not None
    # ------------------------------------------
    # Setup queue for madrid agreements
    q_result: pika.frame.Method = mrsal.setup_queue(queue=QUEUE_EMERGENCY)
    assert q_result is not None

    # Bind queue to exchange with binding key
    qb_result: pika.frame.Method = mrsal.setup_queue_binding(exchange=EXCHANGE, routing_key=ROUTING_KEY, queue=QUEUE_EMERGENCY)
    assert qb_result is not None
    # ------------------------------------------
    # Publisher:
    # Publish NUM_MESSAGES to the queue
    for msg_index in range(NUM_MESSAGES):
        prop = pika.BasicProperties(
            app_id=APP_ID,
            message_id=MESSAGE_ID + str(msg_index),
            content_type=test_config.CONTENT_TYPE,
            content_encoding=test_config.CONTENT_ENCODING,
            delivery_mode=pika.DeliveryMode.Persistent,
            headers=None,
        )
        message = "MRI_" + str(msg_index)
        mrsal.publish_message(exchange=EXCHANGE, routing_key=ROUTING_KEY, message=json.dumps(message), prop=prop)
    # ------------------------------------------
    time.sleep(1)
    # Confirm messages are routed to the queue
    result1 = mrsal.setup_queue(queue=QUEUE_EMERGENCY, passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == NUM_MESSAGES
    # ------------------------------------------
    # Start concurrent consumers
    start_time = time.time()
    mrsal.start_concurrence_consumer(
        total_threads=NUM_THREADS,
        queue=QUEUE_EMERGENCY,
        callback=consumer_callback_with_delivery_info,
        callback_args=(test_config.HOST, QUEUE_EMERGENCY),
        exchange=EXCHANGE,
        exchange_type=EXCHANGE_TYPE,
        routing_key=ROUTING_KEY,
        inactivity_timeout=INACTIVITY_TIMEOUT,
        callback_with_delivery_info=True,
    )
    duration = time.time() - start_time
    log.info(f"Concurrent consumers are done in {duration} seconds")
    # ------------------------------------------
    # Confirm messages are consumed
    result2 = mrsal.setup_queue(queue=QUEUE_EMERGENCY, passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 0

    mrsal.close_connection()


def consumer_callback_with_delivery_info(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    time.sleep(5)
    return True


def consumer_callback(host_param: str, queue_param: str, message_param: str):
    time.sleep(5)
    return True


if __name__ == "__main__":
    test_concurrent_consumer()
