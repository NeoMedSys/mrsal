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


def test_delay_and_dead_letters():
    MESSAGE_TTL: int = 2000  # ms

    EXCHANGE: str = 'agreements'
    EXCHANGE_TYPE: str = 'x-delayed-message'
    EXCHANGE_ARGS: str = {'x-delayed-type': 'direct'}
    QUEUE: str = 'agreements_queue'
    ROUTING_KEY: str = 'agreements_key'

    DL_EXCHANGE: str = 'dl_agreements'
    DL_EXCHANGE_TYPE: str = 'direct'
    DL_QUEUE: str = 'dl_agreements_queue'
    DL_ROUTING_KEY: str = 'dl_agreements_key'

    # Specify dl exchange and dl routing key for queue
    QUEUE_ARGS = {'x-dead-letter-exchange': DL_EXCHANGE,
                  'x-dead-letter-routing-key': DL_ROUTING_KEY}
    # Set time to live for messages in queue
    if MESSAGE_TTL != None:
        QUEUE_ARGS['x-message-ttl'] = MESSAGE_TTL
    # ------------------------------------------

    # Delete existing queues and exchanges to use
    amqp.exchange_delete(exchange=EXCHANGE)
    amqp.exchange_delete(exchange=DL_EXCHANGE)
    amqp.queue_delete(queue=QUEUE)
    amqp.queue_delete(queue=DL_QUEUE)
    # ------------------------------------------

    # Setup dead letters exchange
    exch_result1: pika.frame.Method = amqp.setup_exchange(exchange=DL_EXCHANGE,
                                                          exchange_type=DL_EXCHANGE_TYPE)
    assert exch_result1 != None

    # Setup main exchange with 'x-delayed-message' type
    # and arguments where we specify how the messages will be routed after the delay period specified
    exch_result2: pika.frame.Method = amqp.setup_exchange(exchange=EXCHANGE,
                                                          exchange_type=EXCHANGE_TYPE,
                                                          arguments=EXCHANGE_ARGS)
    assert exch_result2 != None
    # ------------------------------------------

    # Setup main queue with arguments where we specify DL_EXCHANGE and DL_ROUTING_KEY
    q_result1: pika.frame.Method = amqp.setup_queue(queue=QUEUE,
                                                    arguments=QUEUE_ARGS)
    assert q_result1 != None

    # Bind main queue to the main exchange with routing_key
    qb_result1: pika.frame.Method = amqp.setup_queue_binding(exchange=EXCHANGE,
                                                             routing_key=ROUTING_KEY,
                                                             queue=QUEUE)
    assert qb_result1 != None
    # ------------------------------------------

    # Bind DL_QUEUE to DL_EXCHANGE with DL_ROUTING_KEY
    q_result2: pika.frame.Method = amqp.setup_queue(queue=DL_QUEUE)
    assert q_result2 != None

    qb_result2: pika.frame.Method = amqp.setup_queue_binding(exchange=DL_EXCHANGE,
                                                             routing_key=DL_ROUTING_KEY,
                                                             queue=DL_QUEUE)
    assert qb_result2 != None
    # ------------------------------------------

    # Publisher:
    #   Message ("uuid1") is published with x-delay=2000
    #   Message ("uuid2") is published with x-delay=1000
    #   Message ("uuid3") is published with x-delay=3000
    #   Message ("uuid4") is published with x-delay=4000
    x_delay1: int = 2000
    prop1 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'x-delay': x_delay1},
        delivery_mode=pika.DeliveryMode.Persistent)
    message1 = 'uuid1'
    amqp.publish_message(exchange=EXCHANGE,
                         routing_key=ROUTING_KEY,
                         message=json.dumps(message1),
                         properties=prop1)

    x_delay2: int = 1000
    prop2 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'x-delay': x_delay2},
        delivery_mode=pika.DeliveryMode.Persistent)
    message2 = 'uuid2'
    amqp.publish_message(exchange=EXCHANGE,
                         routing_key=ROUTING_KEY,
                         message=json.dumps(message2),
                         properties=prop2)

    x_delay3: int = 3000
    prop3 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'x-delay': x_delay3},
        delivery_mode=pika.DeliveryMode.Persistent)
    message3 = 'uuid3'
    amqp.publish_message(exchange=EXCHANGE,
                         routing_key=ROUTING_KEY,
                         message=json.dumps(message3),
                         properties=prop3)

    x_delay4: int = 4000
    prop4 = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers={'x-delay': x_delay4},
        delivery_mode=pika.DeliveryMode.Persistent)
    message4 = 'uuid4'
    amqp.publish_message(exchange=EXCHANGE,
                         routing_key=ROUTING_KEY,
                         message=json.dumps(message4),
                         properties=prop4)
    # ------------------------------------------

    log.info(f'===== Start consuming from {QUEUE} ========')
    # Consumer from main queue
    #   Message ("uuid2"): Consumed first because its delivered from exchange to the queue
    #     after x-delay=1000ms which is the shortest time.
    #       - This message is rejected by consumer's callback.
    #       - Therefor it will be negatively-acknowledged by consumer.
    #       - Then it will be forwarded to dead-letters-exchange (x-first-death-reason: rejected).
    #   Message ("uuid1"): Consumed at second place because its x-delay = 2000 ms.
    #       - This message is positively-acknowledged by consumer.
    #       - Then it will be deleted from queue.
    #   Message ("uuid3"): Consumed at third place because its x-delay = 3000 ms.
    #       - This message has processing time in the consumer's callback equal to 3s
    #           which is greater that TTL=2s.
    #       - After processing will be positively-acknowledged by consumer.
    #       - Then it will be deleted from queue.
    #   Message ("uuid4"): Consumed at fourth place because its x-delay = 4000 ms.
    #       - This message will be forwarded to dead-letters-exchange
    #           because it spent in the queue more than TTL=2s waiting "uuid3" to be processed
    #           (x-first-death-reason: expired).
    amqp.start_consumer(
        queue=QUEUE,
        callback=consumer_callback,
        callback_args=(test_config.HOST, QUEUE),
        inactivity_timeout=6,
        requeue=False
    )
    # ------------------------------------------

    # Confirm messages are routed to respected queue
    result = amqp.setup_queue(queue=DL_QUEUE)
    message_count = result.method.message_count
    assert message_count == 2
    # ------------------------------------------

    log.info(f'===== Start consuming from {DL_QUEUE} ========')
    # Consumer from dead letters queue
    #   Message ("uuid2"):
    #       - This message is positively-acknowledged by consumer.
    #       - Then it will be deleted from dl-queue.
    #   Message ("uuid4"):
    #       - This message is positively-acknowledged by consumer.
    #       - Then it will be deleted from dl-queue.

    amqp.start_consumer(
        queue=DL_QUEUE,
        callback=consumer_dead_letters_callback,
        callback_args=(test_config.HOST, DL_QUEUE),
        inactivity_timeout=3,
        requeue=False
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result = amqp.setup_queue(queue=DL_QUEUE)
    message_count = result.method.message_count
    log.info(f'Message count in queue "{DL_QUEUE}" after consuming= {message_count}')
    assert message_count == 0

def consumer_callback(host: str, queue: str, message: str):
    if message == 'uuid3':
        time.sleep(3)
    return message != 'uuid2'

def consumer_dead_letters_callback(host_param: str, queue_param: str, message_param: str):
    return True


if __name__ == '__main__':
    test_delay_and_dead_letters()
