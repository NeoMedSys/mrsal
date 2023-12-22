import os

import pika
import pytest
from pika.exchange_type import ExchangeType

import mrsal.config.config as config
import tests.config as test_config
from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)


def test_connection_exceptions():
    failed_host = "not_exist_localhost"
    mrsal1 = Mrsal(host=failed_host, port=config.RABBITMQ_PORT, credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        mrsal1.connect_to_server()

    host = os.environ.get("RABBITMQ_HOST", "localhost")
    failed_v_hold_type = 123
    mrsal2 = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=("root", "password"), virtual_host=failed_v_hold_type)
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        mrsal2.connect_to_server()

    failed_password = "123"
    mrsal3 = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=(config.RABBITMQ_USER, failed_password), virtual_host=config.V_HOST)
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        mrsal3.connect_to_server()

    failed_username = "root1"
    mrsal4 = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=(failed_username, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        mrsal4.connect_to_server()

    failed_port = 123
    mrsal5 = Mrsal(host=host, port=failed_port, credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        mrsal5.connect_to_server()


def test_exchange_exceptions():
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    mrsal = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    mrsal.connect_to_server()

    with pytest.raises(pika.exceptions.ConnectionClosedByBroker):
        mrsal.setup_exchange(exchange=test_config.EXCHANGE, exchange_type="not_exist")

    with pytest.raises(TypeError):
        mrsal.setup_exchange(test_config.EXCHANGE_TYPE)


def test_queue_exceptions():
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    mrsal = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    mrsal.connect_to_server()
    not_exist_queue = "not_exist"
    with pytest.raises(pika.exceptions.ConnectionClosedByBroker):
        mrsal.setup_queue(queue=not_exist_queue, passive=True)


def test_bind_exceptions():
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    mrsal = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    mrsal.connect_to_server()
    exchange = "not_exist_exch"
    queue = "not_exist_queue"
    routing_key = "whatever"
    with pytest.raises(pika.exceptions.ConnectionClosedByBroker):
        mrsal.setup_queue_binding(exchange=exchange, queue=queue, routing_key=routing_key)


def test_active_exchange_exceptions():
    mrsal = Mrsal(host=test_config.HOST, port=config.RABBITMQ_PORT, credentials=config.RABBITMQ_CREDENTIALS, virtual_host=config.V_HOST)
    mrsal.connect_to_server()
    exchange = "not_exist_exch"
    with pytest.raises(pika.exceptions.ConnectionClosedByBroker):
        mrsal.exchange_exist(exchange=exchange, exchange_type=ExchangeType.direct)


def test_active_queue_exceptions():
    mrsal = Mrsal(host=test_config.HOST, port=config.RABBITMQ_PORT, credentials=config.RABBITMQ_CREDENTIALS, virtual_host=config.V_HOST)
    mrsal.connect_to_server()
    queue = "not_exist_queue"
    with pytest.raises(pika.exceptions.ConnectionClosedByBroker):
        mrsal.queue_exist(queue=queue)
