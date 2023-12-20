import json
import time

import pika

import mrsal.config.config as config
import tests.config as test_config
from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)

mrsal = Mrsal(host=test_config.HOST, port=config.RABBITMQ_PORT, credentials=config.RABBITMQ_CREDENTIALS, virtual_host=config.V_HOST, verbose=True)
mrsal.connect_to_server()


def test_redelivery_with_delay():
    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange="agreements")
    mrsal.queue_delete(queue="agreements_queue")
    # ------------------------------------------

    # Setup main exchange with delay type
    exch_result1: pika.frame.Method = mrsal.setup_exchange(exchange="agreements", exchange_type="x-delayed-message", arguments={"x-delayed-type": "direct"})
    assert exch_result1 is not None
    # ------------------------------------------
    # Setup main queue
    q_result1: pika.frame.Method = mrsal.setup_queue(queue="agreements_queue")
    assert q_result1 is not None

    # Bind main queue to the main exchange with routing_key
    qb_result1: pika.frame.Method = mrsal.setup_queue_binding(exchange="agreements", routing_key="agreements_key", queue="agreements_queue")
    assert qb_result1 is not None
    # ------------------------------------------

    """
    Publisher:
      Message ("uuid1") is published with delay 1 sec
      Message ("uuid2") is published with delay 2 sec
    """
    message1 = "uuid1"
    prop1 = pika.BasicProperties(
        app_id="test_delivery-limit",
        message_id="msg_uuid1",
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={"x-delay": 1000, "x-retry-limit": 2},
    )
    mrsal.publish_message(exchange="agreements", routing_key="agreements_key", message=json.dumps(message1), prop=prop1)

    message2 = "uuid2"
    prop2 = pika.BasicProperties(
        app_id="test_delivery-limit",
        message_id="msg_uuid2",
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={"x-delay": 2000, "x-retry-limit": 3, "x-retry": 0},
    )
    mrsal.publish_message(exchange="agreements", routing_key="agreements_key", message=json.dumps(message2), prop=prop2)

    # ------------------------------------------
    # Waiting for the delay time of the messages in the exchange. Then will be
    # delivered to the queue.
    time.sleep(3)

    # Confirm messages are published
    result: pika.frame.Method = mrsal.setup_queue(queue="agreements_queue", passive=True)
    message_count = result.method.message_count
    log.info(f'Message count in queue "agreements_queue" before consuming= {message_count}')
    assert message_count == 2

    log.info('===== Start consuming from "agreements_queue" ========')
    """
    Consumer from main queue
      Message ("uuid1"):
          - This message is positively-acknowledged by consumer.
          - Then it will be deleted from queue.
      Message ("uuid2"):
          - This message is rejected by consumer's callback.
          - Therefor it will be negatively-acknowledged by consumer.
          - Then it will be redelivered with incremented x-retry until, either \
            is acknowledged or x-retry = x-retry-limit.
    """
    mrsal.start_consumer(
        queue="agreements_queue",
        callback=consumer_callback,
        callback_args=(test_config.HOST, "agreements_queue"),
        inactivity_timeout=8,
        requeue=False,
        callback_with_delivery_info=True,
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result: pika.frame.Method = mrsal.setup_queue(queue="agreements_queue", passive=True)
    message_count = result.method.message_count
    log.info(f'Message count in queue "agreements_queue" after consuming= {message_count}')
    assert message_count == 0


def consumer_callback(host: str, queue: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message: str):
    return message != b'"\\"uuid2\\""'


if __name__ == "__main__":
    test_redelivery_with_delay()
