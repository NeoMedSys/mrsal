import os

import rabbitamqp.config.config as config
from rabbitamqp.src.amqp import Amqp
from typing import Tuple

def setup_amqp(host: str, port: int, credentials: Tuple[str, str], virtual_host: str, exchange: str, exchange_type: str,
               queue: str, routing_key: str, dead_letter_queue: str, dead_letter_routing_key: str):
    amqp = Amqp(host=host,
                port=port,
                credentials=credentials,
                virtual_host=virtual_host)

    # Establish connection
    amqp.setup_connection()

    # Create exchange and bind it to the main queue
    amqp.setup_exchange(exchange=exchange, exchange_type=exchange_type)
    amqp.setup_queue(queue=queue, durable=True, exclusive=False, auto_delete=False)
    amqp.setup_queue_binding(exchange=exchange, queue=queue, routing_key=routing_key)

    amqp.setup_queue(queue=dead_letter_queue, durable=True, exclusive=False, auto_delete=False)
    amqp.setup_queue_binding(exchange=exchange, queue=dead_letter_queue, routing_key=dead_letter_routing_key)

    amqp.confirm_delivery()

    return amqp
