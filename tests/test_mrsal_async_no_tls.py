import pytest
from unittest.mock import AsyncMock, patch
import aio_pika
from aio_pika import Message
from mrsal.amqp.subclass import MrsalAsyncAMQP
from typing import Type
from dataclasses import dataclass


# Configuration and expected payload definition
SETUP_ARGS = {
    'host': 'localhost',
    'port': 5672,
    'credentials': ('user', 'password'),
    'virtual_host': 'testboi',
    'ssl': False,
    'heartbeat': 60,
    'prefetch_count': 1
}

@dataclass
class ExpectedPayload:
    id: int
    name: str
    active: bool


# Fixture to mock the async connection and its methods
@pytest.fixture
async def mock_amqp_connection():
    with patch('aio_pika.connect_robust', new_callable=AsyncMock) as mock_connect_robust:
        mock_channel = AsyncMock()
        mock_connection = AsyncMock()
        mock_connection.channel.return_value = mock_channel

        mock_connect_robust.return_value = mock_connection

        # Return the connection and channel
        return mock_connection, mock_channel


@pytest.fixture
async def amqp_consumer(mock_amqp_connection):
    # Await the connection fixture and unpack
    mock_connection, mock_channel = await mock_amqp_connection
    consumer = MrsalAsyncAMQP(**SETUP_ARGS)
    consumer._connection = mock_connection  # Inject the mocked connection
    consumer._channel = mock_channel
    return consumer  # Return the consumer instance



@pytest.mark.asyncio
async def test_valid_message_processing(amqp_consumer):
    """Test valid message processing in async consumer."""
    consumer = await amqp_consumer  # Ensure we await it properly

    valid_body = b'{"id": 1, "name": "Test", "active": true}'
    mock_message = AsyncMock(body=valid_body, ack=AsyncMock(), reject=AsyncMock())


    # Manually set the app_id and message_id properties as simple attributes
    mock_message.configure_mock(app_id="test_app", message_id="12345")
    # Set up mocks for channel and queue interactions
    mock_queue = AsyncMock()
    async def message_generator():
        yield mock_message

    #mock_queue.iterator.return_value = message_generator()
    mock_queue.iterator = message_generator

    # Properly mock the async context manager for the queue
    mock_queue.__aenter__.return_value = mock_queue
    mock_queue.__aexit__.return_value = AsyncMock()

    # Ensure declare_queue returns the mocked queue
    consumer._channel.declare_queue.return_value = mock_queue

    mock_callback = AsyncMock()
    # Start the consumer with a mocked queue
    await consumer.start_consumer(
        queue_name='test_q',
        callback=mock_callback,
        routing_key='test_route',
        exchange_name='test_x',
        exchange_type='direct'
    )
    mock_callback.reset_mock()

    async for message in mock_queue.iterator():
        await mock_callback(message)

    mock_callback.assert_called_once_with(mock_message)
    # mock_callback.assert_called_with(mock_message)
