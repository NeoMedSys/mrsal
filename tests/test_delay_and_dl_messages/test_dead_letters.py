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

def test_dead_letters():

    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange='agreements')
    mrsal.exchange_delete(exchange='dl_agreements')
    mrsal.queue_delete(queue='agreements_queue')
    mrsal.queue_delete(queue='dl_agreements_queue')
    # ------------------------------------------

    # Setup dead letters exchange
    exch_result1: pika.frame.Method = mrsal.setup_exchange(exchange='dl_agreements',
                                                          exchange_type='direct')
    assert exch_result1 != None

    # Setup main exchange
    exch_result2: pika.frame.Method = mrsal.setup_exchange(exchange='agreements',
                                                          exchange_type='direct')
    assert exch_result2 != None
    # ------------------------------------------

    # Setup main queue with arguments where we specify DL_EXCHANGE and DL_ROUTING_KEY
    q_result1: pika.frame.Method = mrsal.setup_queue(queue='agreements_queue',
                                                    arguments={'x-dead-letter-exchange': 'dl_agreements',
                                                               'x-dead-letter-routing-key': 'dl_agreements_key',
                                                               'x-message-ttl': test_config.MESSAGE_TTL})
    assert q_result1 != None

    # Bind main queue to the main exchange with routing_key
    qb_result1: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                             routing_key='agreements_key',
                                                             queue='agreements_queue')
    assert qb_result1 != None
    # ------------------------------------------

    # Bind DL_QUEUE to DL_EXCHANGE with DL_ROUTING_KEY
    q_result2: pika.frame.Method = mrsal.setup_queue(queue='dl_agreements_queue')
    assert q_result2 != None

    qb_result2: pika.frame.Method = mrsal.setup_queue_binding(exchange='dl_agreements',
                                                             routing_key='dl_agreements_key',
                                                             queue='dl_agreements_queue')
    assert qb_result2 != None
    # ------------------------------------------

    """
    Publisher:
      Message ("uuid1") is published
      Message ("uuid2") is published
      Message ("uuid3") is published
      Message ("uuid4") is published
    """
    prop1 = pika.BasicProperties(
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent)
    message1 = 'uuid1'
    mrsal.publish_message(exchange='agreements',
                         routing_key='agreements_key',
                         message=json.dumps(message1),
                         properties=prop1)

    prop2 = pika.BasicProperties(
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent)
    message2 = 'uuid2'
    mrsal.publish_message(exchange='agreements',
                         routing_key='agreements_key',
                         message=json.dumps(message2),
                         properties=prop2)

    prop3 = pika.BasicProperties(
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent)
    message3 = 'uuid3'
    mrsal.publish_message(exchange='agreements',
                         routing_key='agreements_key',
                         message=json.dumps(message3),
                         properties=prop3)

    prop4 = pika.BasicProperties(
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent)
    message4 = 'uuid4'
    mrsal.publish_message(exchange='agreements',
                         routing_key='agreements_key',
                         message=json.dumps(message4),
                         properties=prop4)
    # ------------------------------------------

    log.info(f'===== Start consuming from "agreements_queue" ========')
    """
    Consumer from main queue
      Message ("uuid1"):
          - This message is positively-acknowledged by consumer.
          - Then it will be deleted from queue.
      Message ("uuid2"):
          - This message is rejected by consumer's callback.
          - Therefor it will be negatively-acknowledged by consumer.
          - Then it will be forwarded to dead-letters-exchange (x-first-death-reason: rejected).
      Message ("uuid3"):
          - This message has processing time in the consumer's callback equal to 3s
              which is greater that TTL=2s.
          - After processing will be positively-acknowledged by consumer.
          - Then it will be deleted from queue.
      Message ("uuid4"):
          - This message will be forwarded to dead-letters-exchange
              because it spent in the queue more than TTL=2s waiting "uuid3" to be processed
              (x-first-death-reason: expired).
    """
    mrsal.start_consumer(
        queue='agreements_queue',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'agreements_queue'),
        inactivity_timeout=6,
        requeue=False
    )
    # ------------------------------------------

    # Confirm messages are routed to respected queue
    result = mrsal.setup_queue(queue='dl_agreements_queue')
    message_count = result.method.message_count
    assert message_count == 2
    # ------------------------------------------

    log.info(f'===== Start consuming from "dl_agreements_queue" ========')
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
        requeue=False
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result = mrsal.setup_queue(queue='dl_agreements_queue')
    message_count = result.method.message_count
    log.info(f'Message count in queue "dl_agreements_queue" after consuming= {message_count}')
    assert message_count == 0

def consumer_callback(host: str, queue: str, message: str):
    if message == 'uuid3':
        time.sleep(3)
    return message != 'uuid2'

def consumer_dead_letters_callback(host_param: str, queue_param: str, message_param: str):
    return True


if __name__ == '__main__':
    test_dead_letters()
