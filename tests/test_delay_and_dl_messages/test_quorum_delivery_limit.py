"""
- The quorum queue is a modern queue type for RabbitMQ implementing a durable, replicated FIFO queue based on the Raft consensus algorithm. \
- It is available as of RabbitMQ 3.8.0.\
- It is possible to set a delivery limit for a queue using a policy argument, delivery-limit.\

For more info: https://www.rabbitmq.com/quorum-queues.html
"""

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
              virtual_host=config.V_HOST,
              verbose=True)
mrsal.connect_to_server()

def test_quorum_delivery_limit():

    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange='agreements')
    mrsal.queue_delete(queue='agreements_queue')
    # ------------------------------------------
    queue_arguments = {
        # Queue of quorum type
        'x-queue-type': 'quorum',
        # Set a delivery limit for a queue using a policy argument, delivery-limit.
        # When a message has been returned more times than the limit the message will be dropped \
        # or dead-lettered(if a DLX is configured).
        'x-delivery-limit': 3}

    # ------------------------------------------

    # Setup main exchange
    exch_result1: pika.frame.Method = mrsal.setup_exchange(exchange='agreements',
                                                           exchange_type='direct')
    assert exch_result1 is not None
    # ------------------------------------------

    # Setup main queue with arguments
    q_result1: pika.frame.Method = mrsal.setup_queue(queue='agreements_queue',
                                                     arguments=queue_arguments)
    assert q_result1 is not None

    # Bind main queue to the main exchange with routing_key
    qb_result1: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                              routing_key='agreements_key',
                                                              queue='agreements_queue')
    assert qb_result1 is not None
    # ------------------------------------------

    """
    Publisher:
      Message ("uuid1") is published
      Message ("uuid2") is published
    """
    message1 = 'uuid1'
    prop1 = pika.BasicProperties(
        app_id='test_delivery-limit',
        message_id='msg_uuid1',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
    mrsal.publish_message(exchange='agreements',
                          routing_key='agreements_key',
                          message=json.dumps(message1), prop=prop1)

    message2 = 'uuid2'
    prop2 = pika.BasicProperties(
        app_id='test_delivery-limit',
        message_id='msg_uuid2',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
    mrsal.publish_message(exchange='agreements',
                          routing_key='agreements_key',
                          message=json.dumps(message2), prop=prop2)

    # ------------------------------------------
    time.sleep(1)

    # Confirm messages are published
    result: pika.frame.Method = mrsal.setup_queue(queue='agreements_queue', passive=True,
                                                  arguments=queue_arguments)
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
          - Then it will be redelivered until, either it's acknowledged or x-delivery-limit is reached.
    """
    mrsal.start_consumer(
        queue='agreements_queue',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'agreements_queue'),
        inactivity_timeout=1,
        requeue=True,
        callback_with_delivery_info=True
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result: pika.frame.Method = mrsal.setup_queue(queue='agreements_queue', passive=True,
                                                  arguments=queue_arguments)
    message_count = result.method.message_count
    log.info(f'Message count in queue "agreements_queue" after consuming= {message_count}')
    assert message_count == 0

def consumer_callback(host: str, queue: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message: str):
    return message != b'"\\"uuid2\\""'

def consumer_dead_letters_callback(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    return True


if __name__ == '__main__':
    test_quorum_delivery_limit()
