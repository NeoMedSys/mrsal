import json

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

def test_delay_letter():

    # Delete existing queues and exchanges to use
    amqp.exchange_delete(exchange='agreements')
    amqp.queue_delete(queue='agreements_queue')
    # ------------------------------------------
    # Setup main exchange with 'x-delayed-message' type
    # and arguments where we specify how the messages will be routed after the delay period specified
    exch_result: pika.frame.Method = amqp.setup_exchange(exchange='agreements',
                                                         exchange_type='x-delayed-message',
                                                         arguments={'x-delayed-type': 'direct'})
    assert exch_result != None
    # ------------------------------------------

    # Setup main queue with arguments where we specify DL_EXCHANGE and DL_ROUTING_KEY
    q_result: pika.frame.Method = amqp.setup_queue(queue='agreements_queue')
    assert q_result != None

    # Bind main queue to the main exchange with routing_key
    qb_result: pika.frame.Method = amqp.setup_queue_binding(exchange='agreements',
                                                            routing_key='agreements_key',
                                                            queue='agreements_queue')
    assert qb_result != None
    # ------------------------------------------

    """
    Publisher:
      Message ("uuid1") is published with x-delay=3000
      Message ("uuid2") is published with x-delay=1000
    """
    x_delay1: int = 3000
    message1 = 'uuid1'
    prop1 = pika.BasicProperties(
        content_type=test_config.CONTENT_TYPE,
        headers={'x-delay': x_delay1},
        delivery_mode=pika.DeliveryMode.Persistent)
    amqp.publish_message(exchange='agreements',
                         routing_key='agreements_key',
                         message=json.dumps(message1),
                         properties=prop1)

    x_delay2: int = 1000
    message2 = 'uuid2'
    prop2 = pika.BasicProperties(
        content_encoding=test_config.CONTENT_ENCODING,
        headers={'x-delay': x_delay2},
        delivery_mode=pika.DeliveryMode.Persistent)
    amqp.publish_message(exchange='agreements',
                         routing_key='agreements_key',
                         message=json.dumps(message2),
                         properties=prop2)
    # ------------------------------------------

    log.info('===== Start consuming from "agreements_queue" ========')
    """
    Consumer from main queue
      Message ("uuid2"): Consumed first because its delivered from exchange to the queue
       after x-delay=1000ms which is the shortest time.
      Message ("uuid1"): Consumed at second place because its x-delay = 3000 ms.
    """
    amqp.start_consumer(
        queue='agreements_queue',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'agreements_queue'),
        inactivity_timeout=3,
        requeue=False
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result = amqp.setup_queue(queue='agreements_queue')
    message_count = result.method.message_count
    assert message_count == 0

def consumer_callback(host: str, queue: str, message: str):
    return True


if __name__ == '__main__':
    test_delay_letter()
