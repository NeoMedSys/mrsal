import pytest
from unittest.mock import AsyncMock, patch
from mrsal.amqp.subclass import MrsalAsyncAMQP
from pydantic.dataclasses import dataclass
from pydantic import ValidationError


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


# Fixture to mock the async connection and its methods - SYNC fixture
@pytest.fixture
def mock_amqp_connection():
    with patch('aio_pika.connect_robust', new_callable=AsyncMock) as mock_connect_robust:
        mock_channel = AsyncMock()
        mock_connection = AsyncMock()
        mock_connection.channel.return_value = mock_channel

        mock_connect_robust.return_value = mock_connection

        # Return the connection and channel
        return mock_connection, mock_channel

@pytest.fixture
def amqp_consumer(mock_amqp_connection):
    # No await needed - it's a sync fixture now
    mock_connection, mock_channel = mock_amqp_connection
    consumer = MrsalAsyncAMQP(**SETUP_ARGS)
    consumer._connection = mock_connection  # Inject the mocked connection
    consumer._channel = mock_channel
    return consumer  # Return the consumer instance


@pytest.mark.asyncio
async def test_valid_message_processing(amqp_consumer):
    """Test valid message processing in async consumer."""
    consumer = amqp_consumer  # No await needed

    valid_body = b'{"id": 1, "name": "Test", "active": true}'
    mock_message = AsyncMock(body=valid_body, ack=AsyncMock(), reject=AsyncMock())

    # Manually set the app_id and message_id properties as simple attributes
    mock_message.configure_mock(app_id="test_app", message_id="12345")

    mock_queue = AsyncMock()
    async def message_generator():
        yield mock_message

    mock_queue.iterator = message_generator
    mock_queue.__aenter__.return_value = mock_queue
    mock_queue.__aexit__.return_value = AsyncMock()

    consumer._channel.declare_queue.return_value = mock_queue

    mock_callback = AsyncMock()

    # Call the async method directly to avoid the asyncio.run() issue
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


@pytest.mark.asyncio
async def test_invalid_payload_validation(amqp_consumer):
    """Test invalid payload handling in async consumer."""
    invalid_payload = b'{"id": "wrong_type", "name": 123, "active": "maybe"}'
    consumer = amqp_consumer  # No await needed

    mock_message = AsyncMock(body=invalid_payload, ack=AsyncMock(), reject=AsyncMock())
    mock_message.configure_mock(app_id="test_app", message_id="12345")

    mock_queue = AsyncMock()
    async def message_generator():
        yield mock_message

    mock_queue.iterator = message_generator
    mock_queue.__aenter__.return_value = mock_queue
    mock_queue.__aexit__.return_value = AsyncMock()

    consumer._channel.declare_queue.return_value = mock_queue

    mock_callback = AsyncMock()

    # Call the async method directly to avoid the asyncio.run() issue
    await consumer.start_consumer(
        queue_name='test_q',
        callback=mock_callback,
        routing_key='test_route',
        exchange_name='test_x',
        exchange_type='direct',
        payload_model=ExpectedPayload,
        auto_ack=True
    )

    async for message in mock_queue.iterator():
        with pytest.raises(ValidationError):
            consumer.validate_payload(message.body, ExpectedPayload)
        await mock_callback(message)

    mock_callback.assert_called_once_with(mock_message)


@pytest.mark.asyncio
async def test_requeue_on_invalid_message(amqp_consumer):
    """Test that invalid messages are requeued when auto_ack is False."""
    invalid_payload = b'{"id": "wrong_type", "name": 123, "active": "maybe"}'
    consumer = amqp_consumer  # No await needed

    mock_message = AsyncMock(body=invalid_payload, ack=AsyncMock(), reject=AsyncMock(), nack=AsyncMock())
    mock_message.configure_mock(app_id="test_app", message_id="12345")

    mock_queue = AsyncMock()
    async def message_generator():
        yield mock_message

    mock_queue.iterator = message_generator
    mock_queue.__aenter__.return_value = mock_queue
    mock_queue.__aexit__.return_value = AsyncMock()

    consumer._channel.declare_queue.return_value = mock_queue

    mock_callback = AsyncMock()

    # Call the async method directly to avoid the asyncio.run() issue
    await consumer.start_consumer(
        queue_name='test_q',
        callback=mock_callback,
        routing_key='test_route',
        exchange_name='test_x',
        exchange_type='direct',
        payload_model=ExpectedPayload,
        auto_ack=False
    )

    async for message in mock_queue.iterator():
        with pytest.raises(ValidationError):
            consumer.validate_payload(message.body, ExpectedPayload)
        await message.nack(requeue=True)

    mock_callback.assert_not_called()
    mock_message.nack.assert_called_once_with(requeue=True)
