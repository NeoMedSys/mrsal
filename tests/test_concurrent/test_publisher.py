"""
This test is just a message publisher that can be used after running 
the concurrent consumers in "test_concurrent_consumers.py" to test "inactivity_timeout" 
"""

import concurrent.futures
import json
import time

import pika
from pika.exchange_type import ExchangeType

import mrsal.config.config as config
import tests.config as test_config
from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)

mrsal = Mrsal(host=test_config.HOST,
              port=config.RABBITMQ_PORT,
              credentials=config.RABBITMQ_CREDENTIALS,
              virtual_host=config.V_HOST)
mrsal.connect_to_server()

APP_ID = "TEST_CONCURRENT_CONSUMERS"
EXCHANGE = "CLINIC"
EXCHANGE_TYPE = ExchangeType.direct
QUEUE_EMERGENCY = "EMERGENCY"
NUM_THREADS = 3
NUM_MESSAGES = 2
INACTIVITY_TIMEOUT = 10
ROUTING_KEY = "PROCESS FOR EMERGENCY"
MESSAGE_ID = "HOSPITAL_EMERGENCY_CT_"

def test_concurrent_consumer():
    # ------------------------------------------
    # Publisher:
    # Publish NUM_MESSAGES to the queue
    for msg_index in range(NUM_MESSAGES):
        prop = pika.BasicProperties(
            app_id=APP_ID,
            message_id=MESSAGE_ID + str(msg_index),
            content_type=test_config.CONTENT_TYPE,
            content_encoding=test_config.CONTENT_ENCODING,
            delivery_mode=pika.DeliveryMode.Persistent,
            headers=None)
        message = "CT_" + str(msg_index)
        mrsal.publish_message(exchange=EXCHANGE,
                              routing_key=ROUTING_KEY,
                              message=json.dumps(message), prop=prop)
    # ------------------------------------------

    mrsal.close_connection()



if __name__ == "__main__":
    test_concurrent_consumer()
