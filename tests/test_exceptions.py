import json
import os
from socket import gaierror

import pika
import pytest
import rabbitamqp.config.config as config
from rabbitamqp.config.logging import get_logger
from rabbitamqp.src.amqp import Amqp

import tests.config as test_config

log = get_logger(__name__)

def test_connection_exceptions():
    failed_host = 'failelocalhost'
    amqp1 = Amqp(host=failed_host,
                 port=config.RABBITMQ_DEFAULT_PORT,
                 credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD),
                 virtual_host=config.V_HOST)
    with pytest.raises(gaierror) as err1:
        amqp1.setup_connection()
    assert(f'Caught a socket exception caused by failed host: {err1}')

    host = os.environ.get('RABBITMQ_HOST', 'localhost')
    failed_v_hold_type = 123
    amqp2 = Amqp(host=host,
                 port=config.RABBITMQ_DEFAULT_PORT,
                 credentials=('root', 'password'),
                 virtual_host=failed_v_hold_type)
    with pytest.raises(TypeError) as err2:
        amqp2.setup_connection()
    assert(f'Caught a type error exception caused by failed v_host type: {err2}')

    failed_password = '123'
    amqp3 = Amqp(host=host,
                 port=config.RABBITMQ_DEFAULT_PORT,
                 credentials=(config.RABBITMQ_USER, failed_password),
                 virtual_host=config.V_HOST)
    with pytest.raises(pika.exceptions.AMQPConnectionError) as err3:
        amqp3.setup_connection()
    assert(f'Caught a connection exception caused by failed password: {err3}')

    failed_username = 'root1'
    amqp4 = Amqp(host=host,
                 port=config.RABBITMQ_DEFAULT_PORT,
                 credentials=(failed_username, config.RABBITMQ_PASSWORD),
                 virtual_host=config.V_HOST)
    with pytest.raises(pika.exceptions.AMQPConnectionError) as err4:
        amqp4.setup_connection()
    assert(f'Caught a connection exception caused by failed username: {err4}')

    failed_port = 123
    amqp5 = Amqp(host=host,
                 port=failed_port,
                 credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD),
                 virtual_host=config.V_HOST)
    with pytest.raises(pika.exceptions.AMQPConnectionError) as err5:
        amqp5.setup_connection()
    assert(f'Caught a connection exception caused by failed port: {err5}')


def test_exchange_exceptions():
    host = os.environ.get('RABBITMQ_HOST', 'localhost')
    amqp = Amqp(host=host,
                port=config.RABBITMQ_DEFAULT_PORT,
                credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD),
                virtual_host=config.V_HOST)
    try:
        amqp.setup_connection()

        with pytest.raises(pika.exceptions.ConnectionClosedByBroker) as err1:
            amqp.setup_exchange(exchange=test_config.EXCHANGE,
                                exchange_type='not_exist')
        assert(f'Caught a ConnectionClosedByBroker exception caused by COMMAND_INVALID - unknown exchange type "not_exist": {err1}')

        with pytest.raises(TypeError) as err2:
            amqp.setup_exchange(test_config.EXCHANGE_TYPE)
        assert(f'Caught a type error exception caused by missing 1 required positional argument "exchange": {err2}')
    except pika.exceptions.AMQPConnectionError as err:
        with pytest.raises(AttributeError) as err3:
            amqp.setup_exchange(exchange=test_config.EXCHANGE,
                                exchange_type=test_config.EXCHANGE_TYPE)
        assert(f'Caught a AttributeError exception caused by "NoneType" object has no attribute "exchange_declare": {err2}')

def test_queue_exceptions():
    host = os.environ.get('RABBITMQ_HOST', 'localhost')
    amqp = Amqp(host=host,
                port=config.RABBITMQ_DEFAULT_PORT,
                credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD),
                virtual_host=config.V_HOST)
    try:
        amqp.setup_connection()
        not_exist_queue = 'not_exist'
        with pytest.raises(pika.exceptions.ChannelClosedByBroker) as err1:
            amqp.setup_queue(queue=not_exist_queue, passive=True)
        assert(f'Caught a ChannelClosedByBroker exception caused by NOT_FOUND - no queue "{not_exist_queue}" in vhost "{config.V_HOST}": {err1}')
    except pika.exceptions.AMQPConnectionError as err:
        with pytest.raises(AttributeError) as err2:
            amqp.setup_queue(queue='queue')
        assert(f'Caught a AttributeError exception caused by "NoneType" object has no attribute "queue_declare": {err2}')

def test_bind_exceptions():
    host = os.environ.get('RABBITMQ_HOST', 'localhost')
    amqp = Amqp(host=host,
                port=config.RABBITMQ_DEFAULT_PORT,
                credentials=(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD),
                virtual_host=config.V_HOST)
    try:
        amqp.setup_connection()
        exchange = 'not_exist_exch'
        queue = 'not_exist_queue'
        routing_key = 'whatever'
        with pytest.raises(pika.exceptions.ChannelClosedByBroker) as err1:
            amqp.setup_queue_binding(exchange=exchange, queue=queue, routing_key=routing_key)
        assert(f'Caught a ChannelClosedByBroker exception caused by NOT_FOUND - no exchange "{exchange}" or no queue "{queue}" in vhost "{config.V_HOST}": {err1}')
    except pika.exceptions.AMQPConnectionError as err:
        with pytest.raises(AttributeError) as err2:
            amqp.setup_queue_binding(exchange='exch', queue='queue', routing_key='key')
        assert(f'Caught a AttributeError exception caused by "NoneType" object has no attribute "queue_bind": {err2}')


if __name__ == '__main__':
    test_connection_exceptions()
    test_exchange_exceptions()
    test_bind_exceptions()
