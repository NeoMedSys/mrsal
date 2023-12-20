import os
from socket import gaierror

import pika
import pytest
from pika.exchange_type import ExchangeType

import mrsal.config.config as config
import tests.config as test_config
from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)


def test_connection_exceptions():
    failed_host = "failelocalhost"
    mrsal1 = Mrsal(host=failed_host, port=config.RABBITMQ_PORT, credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    with pytest.raises(gaierror) as err1:
        mrsal1.connect_to_server()
    assert f"Caught a socket exception caused by failed host: {err1}"

    host = os.environ.get("RABBITMQ_HOST", "localhost")
    failed_v_hold_type = 123
    mrsal2 = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=("root", "password"), virtual_host=failed_v_hold_type)
    with pytest.raises(TypeError) as err:
        mrsal2.connect_to_server()
    assert f"Caught a type error exception caused by failed v_host type: {err}"

    failed_password = "123"
    mrsal3 = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=(config.RABBITMQ_USER, failed_password), virtual_host=config.V_HOST)
    with pytest.raises(pika.exceptions.AMQPConnectionError) as err1:
        mrsal3.connect_to_server()
    assert f"Caught a connection exception caused by failed password: {err1}"

    failed_username = "root1"
    mrsal4 = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=(failed_username, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    with pytest.raises(pika.exceptions.AMQPConnectionError) as err2:
        mrsal4.connect_to_server()
    assert f"Caught a connection exception caused by failed username: {err2}"

    failed_port = 123
    mrsal5 = Mrsal(host=host, port=failed_port, credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    with pytest.raises(pika.exceptions.AMQPConnectionError) as err3:
        mrsal5.connect_to_server()
    assert f"Caught a connection exception caused by failed port: {err3}"


def test_exchange_exceptions():
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    mrsal = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    try:
        mrsal.connect_to_server()

        with pytest.raises(pika.exceptions.ConnectionClosedByBroker) as err1:
            mrsal.setup_exchange(exchange=test_config.EXCHANGE, exchange_type="not_exist")
        assert f'Caught a ConnectionClosedByBroker exception caused by COMMAND_INVALID - \
                unknown exchange type "not_exist": {err1}'

        with pytest.raises(TypeError) as err2:
            mrsal.setup_exchange(test_config.EXCHANGE_TYPE)
        assert f'Caught a type error exception caused by missing 1 required positional \
                argument "exchange": {err2}'
    except pika.exceptions.AMQPConnectionError:
        with pytest.raises(AttributeError):
            mrsal.setup_exchange(exchange=test_config.EXCHANGE, exchange_type=test_config.EXCHANGE_TYPE)
        assert f'Caught a AttributeError exception caused by "NoneType" object has no \
                attribute "exchange_declare": {err2}'


def test_queue_exceptions():
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    mrsal = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    try:
        mrsal.connect_to_server()
        not_exist_queue = "not_exist"
        with pytest.raises(pika.exceptions.ChannelClosedByBroker) as err1:
            mrsal.setup_queue(queue=not_exist_queue, passive=True)
        assert f'Caught a ChannelClosedByBroker exception caused by NOT_FOUND - \
                no queue "{not_exist_queue}" in vhost "{config.V_HOST}": {err1}'
    except pika.exceptions.AMQPConnectionError:
        with pytest.raises(AttributeError) as err2:
            mrsal.setup_queue(queue="queue")
        assert f'Caught a AttributeError exception caused by "NoneType" object has no \
                attribute "queue_declare": {err2}'


def test_bind_exceptions():
    host = os.environ.get("RABBITMQ_HOST", "localhost")
    mrsal = Mrsal(host=host, port=config.RABBITMQ_PORT, credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD), virtual_host=config.V_HOST)
    try:
        mrsal.connect_to_server()
        exchange = "not_exist_exch"
        queue = "not_exist_queue"
        routing_key = "whatever"
        with pytest.raises(pika.exceptions.ChannelClosedByBroker) as err1:
            mrsal.setup_queue_binding(exchange=exchange, queue=queue, routing_key=routing_key)
        assert f'Caught a ChannelClosedByBroker exception caused by NOT_FOUND - \
                no exchange "{exchange}" or no queue "{queue}" in vhost \
                    "{config.V_HOST}": {err1}'
    except pika.exceptions.AMQPConnectionError:
        with pytest.raises(AttributeError) as err2:
            mrsal.setup_queue_binding(exchange="exch", queue="queue", routing_key="key")
        assert f'Caught a AttributeError exception caused by "NoneType" object has \
            no attribute "queue_bind": {err2}'


def test_active_exchange_exceptions():
    mrsal = Mrsal(host=test_config.HOST, port=config.RABBITMQ_PORT, credentials=config.RABBITMQ_CREDENTIALS, virtual_host=config.V_HOST)
    mrsal.connect_to_server()
    exchange = "not_exist_exch"
    with pytest.raises(pika.exceptions.ChannelClosedByBroker):
        mrsal.exchange_exist(exchange=exchange, exchange_type=ExchangeType.direct)


def test_active_queue_exceptions():
    mrsal = Mrsal(host=test_config.HOST, port=config.RABBITMQ_PORT, credentials=config.RABBITMQ_CREDENTIALS, virtual_host=config.V_HOST)
    mrsal.connect_to_server()
    queue = "not_exist_queue"
    with pytest.raises(pika.exceptions.ChannelClosedByBroker):
        mrsal.queue_exist(queue=queue)


if __name__ == "__main__":
    test_connection_exceptions()
    test_exchange_exceptions()
    test_bind_exceptions()
    test_active_exchange_exceptions()
    test_active_queue_exceptions()
