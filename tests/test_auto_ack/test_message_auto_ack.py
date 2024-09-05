import json
import time
import pika
from loguru import logger as log

import mrsal.config.config as config
import tests.config as test_config
from mrsal.mrsal import Mrsal

mrsal = Mrsal(host=test_config.HOST, port=config.RABBITMQ_PORT, credentials=config.RABBITMQ_CREDENTIALS, virtual_host=config.V_HOST)
mrsal.connect_to_server()


def get_all_messages(mrsal_obj: Mrsal, queue_name):
    messages = []
    while True:
        # Get a message
        method_frame, _header_frame, body = mrsal_obj._channel.basic_get(queue=queue_name)

        # If no more messages, break from the loop
        if method_frame is None:
            break

        # Add the message to the list
        enc_payload = json.loads(body)
        mrsal_msg = enc_payload if isinstance(enc_payload, dict) else json.loads(enc_payload)
        log.info(f"Received message {mrsal_msg}")
        messages.append(mrsal_msg)

        # Acknowledge the message (optional, depending on your use case)
        # channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    return messages


def test_message_auto_ack():
    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange="agreements")
    mrsal.queue_delete(queue="agreements_berlin_queue")
    mrsal.queue_delete(queue="agreements_madrid_queue")
    # ------------------------------------------

    # Setup exchange
    exch_result: pika.frame.Method = mrsal.setup_exchange(exchange="agreements", exchange_type="direct")
    assert exch_result is not None
    # ------------------------------------------

    # Setup queue for berlin agreements
    q_result1: pika.frame.Method = mrsal.setup_queue(queue="agreements_berlin_queue")
    assert q_result1 is not None

    # Bind queue to exchange with binding key
    qb_result1: pika.frame.Method = mrsal.setup_queue_binding(exchange="agreements", routing_key="berlin agreements", queue="agreements_berlin_queue")
    assert qb_result1 is not None
    # ------------------------------------------

    # Setup queue for madrid agreements
    q_result2: pika.frame.Method = mrsal.setup_queue(queue="agreements_madrid_queue")
    assert q_result2 is not None

    # Bind queue to exchange with binding key
    qb_result2: pika.frame.Method = mrsal.setup_queue_binding(exchange="agreements", routing_key="madrid agreements", queue="agreements_madrid_queue")
    assert qb_result2 is not None
    # ------------------------------------------

    # Publisher:
    prop1 = pika.BasicProperties(
        app_id="test_exchange_direct",
        message_id="madrid_uuid",
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None,
    )

    # Message ("uuid2") is published to the exchange and it's routed to queue2
    message_madrid = "uuid_madrid"
    mrsal.publish_message(exchange="agreements", routing_key="madrid agreements", message=json.dumps(message_madrid), prop=prop1)

    prop2 = pika.BasicProperties(
        app_id="test_exchange_direct",
        message_id="berlin_uuid",
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None,
    )
    # Message ("uuid1") is published to the exchange and it's routed to queue1
    message_berlin = "uuid_berlin"
    mrsal.publish_message(exchange="agreements", routing_key="berlin agreements", message=json.dumps(message_berlin), prop=prop2)
    # ------------------------------------------

    time.sleep(1)
    # Confirm messages are routed to respected queues
    result1 = mrsal.setup_queue(queue="agreements_berlin_queue", passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 1

    result2 = mrsal.setup_queue(queue="agreements_madrid_queue", passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 1
    # ------------------------------------------

    # The message is not being processed correctly by the callback method, yet it remains in the queue.
    # This is due to the fact that the message is not auto-acknowledged during consumption (auto_ack=False),
    # and it is also not rejected when it is not processed correctly by the callback method (reject_unprocessed=False).
    mrsal.start_consumer(
        queue="agreements_berlin_queue",
        callback=consumer_callback_berlin,
        callback_args=(test_config.HOST, "agreements_berlin_queue"),
        inactivity_timeout=1,
        requeue=False,
        auto_ack=False,
        reject_unprocessed=False,
        callback_with_delivery_info=True,
    )

    # The message is not being processed correctly by the callback method, but it deleted from the queue.
    # This is due to the fact that the message is auto-acknowledged during consumption (auto_ack=True),
    mrsal.start_consumer(
        queue="agreements_madrid_queue",
        callback=consumer_callback_madrid,
        callback_args=("agreements_madrid_queue",),
        inactivity_timeout=1,
        requeue=False,
        auto_ack=True,
        reject_unprocessed=False,
        callback_with_delivery_info=True,
    )
    # ------------------------------------------
    mrsal.close_connection()

    mrsal_obj = Mrsal(host=test_config.HOST, port=config.RABBITMQ_PORT, credentials=config.RABBITMQ_CREDENTIALS, virtual_host=config.V_HOST)
    mrsal_obj.connect_to_server()
    berlin_messages = get_all_messages(mrsal_obj=mrsal_obj, queue_name="agreements_berlin_queue")
    madrid_messages = get_all_messages(mrsal_obj=mrsal_obj, queue_name="agreements_madrid_queue")

    print(f"--> berlin_messages={berlin_messages}")
    print(f"--> madrid_messages={madrid_messages}")
    assert len(berlin_messages) == 1
    assert berlin_messages[0] == message_berlin
    assert len(madrid_messages) == 0

    mrsal_obj.close_connection()


def consumer_callback_berlin(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    time.sleep(1)
    return False


def consumer_callback_madrid(queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    time.sleep(1)
    return False

