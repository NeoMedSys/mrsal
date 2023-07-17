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


def test_delay_and_dead_letters():

    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange='agreements')
    mrsal.exchange_delete(exchange='dl_agreements')
    mrsal.queue_delete(queue='agreements_queue')
    mrsal.queue_delete(queue='dl_agreements_queue')
    # ------------------------------------------

    # Setup dead letters exchange
    exch_result1: pika.frame.Method = mrsal.setup_exchange(exchange='dl_agreements',
                                                           exchange_type='direct')
    assert exch_result1 is not None

    # Setup main exchange with 'x-delayed-message' type
    # and arguments where we specify how the messages will be routed after the delay period specified
    exch_result2: pika.frame.Method = mrsal.setup_exchange(exchange='agreements',
                                                           exchange_type='x-delayed-message',
                                                           arguments={'x-delayed-type': 'direct'})
    assert exch_result2 is not None
    # ------------------------------------------

    # Setup main queue with arguments where we specify DL_EXCHANGE, DL_ROUTING_KEY and TTL
    q_result1: pika.frame.Method = mrsal.setup_queue(queue='agreements_queue',
                                                     arguments={'x-dead-letter-exchange': 'dl_agreements',
                                                                'x-dead-letter-routing-key': 'dl_agreements_key',
                                                                'x-message-ttl': test_config.MESSAGE_TTL})
    assert q_result1 is not None

    # Bind main queue to the main exchange with routing_key
    qb_result1: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                              routing_key='agreements_key',
                                                              queue='agreements_queue')
    assert qb_result1 is not None
    # ------------------------------------------

    # Bind DL_QUEUE to DL_EXCHANGE with DL_ROUTING_KEY
    q_result2: pika.frame.Method = mrsal.setup_queue(queue='dl_agreements_queue')
    assert q_result2 is not None

    qb_result2: pika.frame.Method = mrsal.setup_queue_binding(exchange='dl_agreements',
                                                              routing_key='dl_agreements_key',
                                                              queue='dl_agreements_queue')
    assert qb_result2 is not None
    # ------------------------------------------

    """
    Publisher:
      Message ("uuid1") is published with x-delay=2000
      Message ("uuid2") is published with x-delay=1000
      Message ("uuid3") is published with x-delay=3000
      Message ("uuid4") is published with x-delay=4000
    """
    x_delay1: int = 2000  # ms
    message1 = 'uuid1'
    prop1 = pika.BasicProperties(
        app_id='test_exchange_dead_and_delay_letters',
        message_id='uuid1_2000ms',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': x_delay1})
    mrsal.publish_message(exchange='agreements',
                          routing_key='agreements_key',
                          message=json.dumps(message1), prop=prop1)

    x_delay2: int = 1000
    message2 = 'uuid2'
    prop2 = pika.BasicProperties(
        app_id='test_exchange_dead_and_delay_letters',
        message_id='uuid2_1000ms',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': x_delay2})
    mrsal.publish_message(exchange='agreements',
                          routing_key='agreements_key',
                          message=json.dumps(message2), prop=prop2)

    x_delay3: int = 3000
    message3 = 'uuid3'
    prop3 = pika.BasicProperties(
        app_id='test_exchange_dead_and_delay_letters',
        message_id='uuid3_3000ms',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': x_delay3})
    mrsal.publish_message(exchange='agreements',
                          routing_key='agreements_key',
                          message=json.dumps(message3), prop=prop3)

    x_delay4: int = 4000
    message4 = 'uuid4'
    prop4 = pika.BasicProperties(
        app_id='test_exchange_dead_and_delay_letters',
        message_id='uuid4_4000ms',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': x_delay4})
    mrsal.publish_message(exchange='agreements',
                          routing_key='agreements_key',
                          message=json.dumps(message4), prop=prop4)
    # ------------------------------------------

    log.info('===== Start consuming from "agreements_queue" ========')
    """
    Consumer from main queue
      Message ("uuid2"): Consumed first because its delivered from exchange to the queue
        after x-delay=1000ms which is the shortest time.
          - This message is rejected by consumer's callback.
          - Therefor it will be negatively-acknowledged by consumer.
          - Then it will be forwarded to dead-letters-exchange (x-first-death-reason: rejected).
      Message ("uuid1"): Consumed at second place because its x-delay = 2000 ms.
          - This message is positively-acknowledged by consumer.
          - Then it will be deleted from queue.
      Message ("uuid3"): Consumed at third place because its x-delay = 3000 ms.
          - This message has processing time in the consumer's callback equal to 3s
              which is greater that TTL=2s.
          - After processing will be positively-acknowledged by consumer.
          - Then it will be deleted from queue.
      Message ("uuid4"): Consumed at fourth place because its x-delay = 4000 ms.
          - This message will be forwarded to dead-letters-exchange
              because it spent in the queue more than TTL=2s waiting "uuid3" to be processed
              (x-first-death-reason: expired).
    """
    mrsal.start_consumer(
        queue='agreements_queue',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'agreements_queue'),
        inactivity_timeout=6,
        requeue=False,
        callback_with_delivery_info=True
    )
    # ------------------------------------------

    # Confirm messages are routed to respected queue
    result = mrsal.setup_queue(queue='dl_agreements_queue')
    message_count = result.method.message_count
    assert message_count == 2
    # ------------------------------------------

    log.info('===== Start consuming from "dl_agreements_queue" ========')
    """
    Consumer from dead letters queue
      Message ("uuid2"):
          - This message is positively-acknowledged by consumer.
          - Then it will be deleted from dl-queue.
      Message ("uuid4"):
          - This message is positively-acknowledged by consumer.
          - Then it will be deleted from dl-queue.
    """

    mrsal.start_consumer(
        queue='dl_agreements_queue',
        callback=consumer_dead_letters_callback,
        callback_args=(test_config.HOST, 'dl_agreements_queue'),
        inactivity_timeout=3,
        requeue=False,
        callback_with_delivery_info=True
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result = mrsal.setup_queue(queue='dl_agreements_queue')
    message_count = result.method.message_count
    log.info(f'Message count in queue "dl_agreements_queue" after consuming= {message_count}')
    assert message_count == 0


def consumer_callback(host: str, queue: str, method_frame: pika.spec.Basic.Deliver,
                      properties: pika.spec.BasicProperties, message: str):
    if message == b'"\\"uuid3\\""':
        time.sleep(3)
    return message != b'"\\"uuid2\\""'


def consumer_dead_letters_callback(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver,
                                   properties: pika.spec.BasicProperties, message_param: str):
    return True


if __name__ == '__main__':
    test_delay_and_dead_letters()
