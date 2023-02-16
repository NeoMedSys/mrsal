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

def test_topic_exchange_workflow():

    ROUTING_KEY_1: str = 'agreements.eu.berlin.august.2022'  # Messages will published with this routing key
    ROUTING_KEY_2: str = 'agreements.eu.madrid.september.2022'  # Messages will published with this routing key

    BINDING_KEY_1: str = 'agreements.eu.berlin.#'  # Berlin agreements
    BINDING_KEY_2: str = 'agreements.*.*.september.#'  # Agreements of september
    BINDING_KEY_3: str = 'agreements.#'  # All agreements
    # ------------------------------------------

    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange='agreements')
    mrsal.queue_delete(queue='berlin_agreements')
    mrsal.queue_delete(queue='september_agreements')

    # Setup exchange
    exch_result: pika.frame.Method = mrsal.setup_exchange(exchange='agreements',
                                                         exchange_type='topic')
    assert exch_result != None
    # ------------------------------------------

    # Setup queue for berlin agreements
    q_result1: pika.frame.Method = mrsal.setup_queue(queue='berlin_agreements')
    assert q_result1 != None

    # Bind queue to exchange with binding key
    qb_result1: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                             routing_key=BINDING_KEY_1,
                                                             queue='berlin_agreements')
    assert qb_result1 != None
    # ----------------------------------

    # Setup queue for september agreements
    q_result2: pika.frame.Method = mrsal.setup_queue(queue='september_agreements')
    assert q_result2 != None

    # Bind queue to exchange with binding key
    qb_result2: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                             routing_key=BINDING_KEY_2,
                                                             queue='september_agreements')
    assert qb_result2 != None
    # ----------------------------------

    # Publisher:

    # Message ("uuid1") is published to the exchange will be routed to queue1
    message1 = 'uuid1'
    prop1 = pika.BasicProperties(
        app_id='test_exchange_topic',
        message_id='berlin',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
    mrsal.publish_message(exchange='agreements',
                         routing_key=ROUTING_KEY_1,
                         message=json.dumps(message1), prop=prop1)

    # Message ("uuid2") is published to the exchange will be routed to queue2
    message2 = 'uuid2'
    prop2 = pika.BasicProperties(
        app_id='test_exchange_topic',
        message_id='september',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
    mrsal.publish_message(exchange='agreements',
                         routing_key=ROUTING_KEY_2,
                         message=json.dumps(message2), prop=prop2)
    # ----------------------------------

    time.sleep(1)
    # Confirm messages are routed to respected queues
    result1 = mrsal.setup_queue(queue='berlin_agreements', passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 1

    result2 = mrsal.setup_queue(queue='september_agreements', passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 1
    # ------------------------------------------

    # Start consumer for every queue
    mrsal.start_consumer(
        queue='berlin_agreements',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'berlin_agreements'),
        inactivity_timeout=1,
        requeue=False
    )

    mrsal.start_consumer(
        queue='september_agreements',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'september_agreements'),
        inactivity_timeout=1,
        requeue=False
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result1 = mrsal.setup_queue(queue='berlin_agreements', passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == 0

    result2 = mrsal.setup_queue(queue='september_agreements', passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 0

def consumer_callback(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    return True


if __name__ == '__main__':
    test_topic_exchange_workflow()
