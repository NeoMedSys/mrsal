import json
import time
import pika

import mrsal.config.config as config
import tests.config as test_config
from mrsal.mrsal import Mrsal


mrsal = Mrsal(host=test_config.HOST, port=config.RABBITMQ_PORT, credentials=config.RABBITMQ_CREDENTIALS, virtual_host=config.V_HOST)
mrsal.connect_to_server()


def test_basic_workflow():
    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange="friendship")
    mrsal.queue_delete(queue="friendship_queue")
    # ------------------------------------------

    # Publisher:
    prop = pika.BasicProperties(
        app_id="friendship_app", message_id="friendship_msg", content_type="text/plain", content_encoding="utf-8", delivery_mode=pika.DeliveryMode.Persistent, headers=None
    )

    # Message is published to the exchange and it's routed to queue
    message_body = "Hello"
    mrsal.publish_message(
        exchange="friendship", exchange_type="direct", queue="friendship_queue", routing_key="friendship_key", message=json.dumps(message_body), prop=prop, fast_setup=True
    )
    # ------------------------------------------

    time.sleep(1)
    # Confirm messages are routed to respected queue.
    result1 = mrsal.setup_queue(queue="friendship_queue", passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 1
    # ------------------------------------------

    # Start consumer.
    mrsal.start_consumer(
        queue="friendship_queue",
        callback=consumer_callback_with_delivery_info,
        callback_args=(test_config.HOST, "friendship_queue"),
        inactivity_timeout=1,
        requeue=False,
        callback_with_delivery_info=True,
    )

    # ------------------------------------------

    # Confirm messages are consumed
    result1 = mrsal.setup_queue(queue="friendship_queue", passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 0


def consumer_callback_with_delivery_info(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    str_message = json.loads(message_param).replace('"', "")
    if "Hello" in str_message:
        app_id = properties.app_id
        msg_id = properties.message_id
        print(f"app_id={app_id}, msg_id={msg_id}")
        print("Salaam habibi")
        return True  # Consumed message processed correctly
    return False


def consumer_callback(host_param: str, queue_param: str, message_param: str):
    str_message = json.loads(message_param).replace('"', "")
    if "Hello" in str_message:
        print("Salaam habibi")
        return True  # Consumed message processed correctly
    return False
