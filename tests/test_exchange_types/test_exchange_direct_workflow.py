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

def test_direct_exchange_workflow():

    # Delete existing queues and exchanges to use
    amqp.exchange_delete(exchange='agreements')
    amqp.queue_delete(queue='agreements_berlin_queue')
    amqp.queue_delete(queue='agreements_madrid_queue')
    # ------------------------------------------

    # Setup exchange
    exch_result: pika.frame.Method = amqp.setup_exchange(exchange='agreements',
                                                         exchange_type='direct')
    assert exch_result != None
    # ------------------------------------------

    # Setup queue for berlin agreements
    q_result1: pika.frame.Method = amqp.setup_queue(queue='agreements_berlin_queue')
    assert q_result1 != None

    # Bind queue to exchange with binding key
    qb_result1: pika.frame.Method = amqp.setup_queue_binding(exchange='agreements',
                                                             routing_key='berlin agreements',
                                                             queue='agreements_berlin_queue')
    assert qb_result1 != None
    # ------------------------------------------

    # Setup queue for madrid agreements
    q_result2: pika.frame.Method = amqp.setup_queue(queue='agreements_madrid_queue')
    assert q_result2 != None

    # Bind queue to exchange with binding key
    qb_result2: pika.frame.Method = amqp.setup_queue_binding(exchange='agreements',
                                                             routing_key='madrid agreements',
                                                             queue='agreements_madrid_queue')
    assert qb_result2 != None
    # ------------------------------------------

    # Publisher:
    prop = pika.BasicProperties(
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent)

    # Message ("uuid2") is published to the exchange and it's routed to queue2
    message2 = 'uuid2'
    amqp.publish_message(exchange='agreements',
                         routing_key='madrid agreements',
                         message=json.dumps(message2),
                         properties=prop)

    # Message ("uuid1") is published to the exchange and it's routed to queue1
    message1 = 'uuid1'
    amqp.publish_message(exchange='agreements',
                         routing_key='berlin agreements',
                         message=json.dumps(message1),
                         properties=prop)
    # ------------------------------------------

    time.sleep(1)
    # Confirm messages are routed to respected queues
    result1 = amqp.setup_queue(queue='agreements_berlin_queue', passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 1

    result2 = amqp.setup_queue(queue='agreements_madrid_queue', passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 1
    # ------------------------------------------

    # Start consumer for every queue
    amqp.start_consumer(
        queue='agreements_berlin_queue',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'agreements_berlin_queue'),
        inactivity_timeout=1,
        requeue=False
    )

    amqp.start_consumer(
        queue='agreements_madrid_queue',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'agreements_madrid_queue'),
        inactivity_timeout=1,
        requeue=False
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result1 = amqp.setup_queue(queue='agreements_berlin_queue', passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 0

    result2 = amqp.setup_queue(queue='agreements_madrid_queue', passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 0

def consumer_callback(host_param: str, queue_param: str, message_param: str):
    return True


if __name__ == '__main__':
    test_direct_exchange_workflow()
