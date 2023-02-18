import json
import time
import pika

import mrsal.config.config as config
import tests.config as test_config
from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)

mrsal = Mrsal(
    # host=test_config.HOST,
    host=config.RABBIT_DOMAIN,
    port=config.RABBITMQ_PORT_TLS,
    ssl=True,
    credentials=config.RABBITMQ_CREDENTIALS,
    virtual_host=config.V_HOST
)


mrsal.connect_to_server()

def test_fast_setup():

    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange='friendship')
    mrsal.queue_delete(queue='friendship_queue')
    # ------------------------------------------

    prop = pika.BasicProperties(
            app_id='test_fast_setup',
            message_id='fast_setup',
            content_type=test_config.CONTENT_TYPE,
            content_encoding=test_config.CONTENT_ENCODING,
            delivery_mode=pika.DeliveryMode.Persistent,
            headers=None)

    mrsal.publish_message(
        exchange='friendship',
        exchange_type='direct',
        routing_key='friendship_key',
        queue='friendship_queue',
        message=json.dumps('Salaam habibi'),
        fast_setup=True,
        prop=prop
    )
    # ------------------------------------------

    # Confirm message is routed to respected queue
    time.sleep(1)
    result = mrsal.setup_queue(queue='friendship_queue')
    message_count = result.method.message_count
    assert message_count == 1
    # ------------------------------------------

    mrsal.start_consumer(
        exchange='friendship',
        exchange_type='direct',
        routing_key='friendship_key',
        queue='friendship_queue',
        callback=consumer_callback,
        callback_args=('localhost', 'friendship_queue'),
        inactivity_timeout=1,
        callback_with_delivery_info=True
    )
    # ------------------------------------------

    # Confirm message is consumed from queue
    result = mrsal.setup_queue(queue='friendship_queue')
    message_count = result.method.message_count
    assert message_count == 0


def consumer_callback(host: str, queue: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, bin_message: str):
    str_message = json.loads(bin_message).replace('"', '')
    if 'Salaam' in str_message:
        return True  # Consumed message processed correctly
    return False


if __name__ == '__main__':
    test_fast_setup()
