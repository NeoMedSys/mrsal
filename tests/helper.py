import os

import rabbitamqptest3.config.config as config
from rabbitamqptest3.src.amqp import Amqp
from typing import Tuple

def setup_amqp(host: str, port: int, credentials: Tuple[str, str], exchange: str, exchange_type: str,
               queue: str, routing_key: str, dead_letter_queue: str, dead_letter_routing_key:str):
    amqp = Amqp(host=host,
                port=port,
                credentials=credentials)

    # Establish connection
    amqp.establish_connection()

    # Create exchange and bind it to the main queue
    amqp.exchange_declare(exchange=exchange, exchange_type=exchange_type)
    amqp.queue_declare(queue=queue, durable=True, exclusive=False, auto_delete=False)
    amqp.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key)

    amqp.queue_declare(queue=dead_letter_queue, durable=True, exclusive=False, auto_delete=False)
    amqp.queue_bind(exchange=exchange, queue=dead_letter_queue, routing_key=dead_letter_routing_key)

    amqp.confirm_delivery()

    return amqp
