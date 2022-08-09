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

def test_direct_exchange_workflow():
    EXCHANGE: str = 'agreements'
    EXCHANGE_TYPE: str = 'direct'
    QUEUE_1: str = 'agreements_berlin_queue'
    QUEUE_2: str = 'agreements_madrid_queue'

    # Messages will published with this routing key
    ROUTING_KEY_1: str = 'berlin agreements'
    ROUTING_KEY_2: str = 'madrid agreements'
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

    # Setup queue for berlin agreements
    q_result1: pika.frame.Method = amqp.setup_queue(queue=QUEUE_1)
    assert q_result1 != None

    # Bind queue to exchange with binding key
    qb_result1: pika.frame.Method = amqp.setup_queue_binding(exchange=EXCHANGE,
                                                             routing_key=ROUTING_KEY_1,
                                                             queue=QUEUE_1)
    assert qb_result1 != None
    # ------------------------------------------

    # Setup queue for madrid agreements
    q_result2: pika.frame.Method = amqp.setup_queue(queue=QUEUE_2)
    assert q_result2 != None

    # Bind queue to exchange with binding key
    qb_result2: pika.frame.Method = amqp.setup_queue_binding(exchange=EXCHANGE,
                                                             routing_key=ROUTING_KEY_2,
                                                             queue=QUEUE_2)
    assert qb_result2 != None
    # ------------------------------------------

    # Publisher:
    prop = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent)

    # Message ("uuid2") is published to the exchange and it's routed to queue2
    message2 = 'uuid2'
    amqp.publish_message(exchange=EXCHANGE,
                         routing_key=ROUTING_KEY_2,
                         message=json.dumps(message2),
                         properties=prop)

    # Message ("uuid1") is published to the exchange and it's routed to queue1
    message1 = 'uuid1'
    amqp.publish_message(exchange=EXCHANGE,
                         routing_key=ROUTING_KEY_1,
                         message=json.dumps(message1),
                         properties=prop)
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
        inactivity_timeout=1,
        requeue=False
    )

    amqp.start_consumer(
        queue=QUEUE_2,
        callback=consumer_callback,
        callback_args=(test_config.HOST, QUEUE_2),
        inactivity_timeout=1,
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
    test_direct_exchange_workflow()
