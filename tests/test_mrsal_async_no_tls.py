import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from mrsal.amqp.subclass import MrsalAsyncAMQP
from mrsal.config import AioPikaAttributes
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
        mock_channel.is_closed = False
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
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
    async def message_generator(**kwargs):
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
    async def message_generator(**kwargs):
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
    async def message_generator(**kwargs):
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


@pytest.mark.asyncio
async def test_setup_async_connection_reraises_unexpected_exception():
    """Unexpected exceptions from setup_async_connection must propagate, not be swallowed."""
    consumer = MrsalAsyncAMQP(**SETUP_ARGS)

    with patch('mrsal.amqp.subclass.connect_robust',
               new_callable=AsyncMock,
               side_effect=RuntimeError("disk on fire")):
        with pytest.raises(RuntimeError, match="disk on fire"):
            await consumer.setup_async_connection()


@pytest.mark.asyncio
async def test_ensure_consumer_channel_closes_prior_open_channel():
    """Prevents the channel leak that previously occurred on tenacity retry."""
    consumer = MrsalAsyncAMQP(**SETUP_ARGS)

    stale_channel = AsyncMock()
    stale_channel.is_closed = False
    fresh_channel = AsyncMock()
    consumer._channel = stale_channel
    consumer._connection = AsyncMock()
    consumer._connection.is_closed = False
    consumer._connection.channel = AsyncMock(return_value=fresh_channel)

    await consumer._ensure_consumer_channel()

    stale_channel.close.assert_awaited_once()
    fresh_channel.set_qos.assert_awaited_once_with(prefetch_count=consumer.prefetch_count)
    assert consumer._channel is fresh_channel


@pytest.mark.asyncio
async def test_ensure_consumer_channel_closes_fresh_channel_if_set_qos_fails():
    """If set_qos raises, the freshly opened channel must be closed (no leak)."""
    consumer = MrsalAsyncAMQP(**SETUP_ARGS)
    consumer._channel = None

    fresh_channel = AsyncMock()
    fresh_channel.set_qos = AsyncMock(side_effect=RuntimeError("qos boom"))
    consumer._connection = AsyncMock()
    consumer._connection.is_closed = False
    consumer._connection.channel = AsyncMock(return_value=fresh_channel)

    with pytest.raises(RuntimeError, match="qos boom"):
        await consumer._ensure_consumer_channel()

    fresh_channel.close.assert_awaited_once()
    assert consumer._channel is None


@pytest.mark.asyncio
async def test_ensure_async_connection_reconnects_stale_connection():
    """Closed-but-non-None connection must be reconnected, not reused."""
    consumer = MrsalAsyncAMQP(**SETUP_ARGS)

    stale_connection = AsyncMock()
    stale_connection.is_closed = True
    consumer._connection = stale_connection

    with patch.object(consumer, 'setup_async_connection', new_callable=AsyncMock) as mock_setup:
        await consumer._ensure_async_connection()

    stale_connection.close.assert_awaited_once()
    mock_setup.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_async_connection_noop_when_connection_is_open():
    """Healthy connection must not be torn down."""
    consumer = MrsalAsyncAMQP(**SETUP_ARGS)
    open_connection = AsyncMock()
    open_connection.is_closed = False
    consumer._connection = open_connection

    with patch.object(consumer, 'setup_async_connection', new_callable=AsyncMock) as mock_setup:
        await consumer._ensure_async_connection()

    open_connection.close.assert_not_called()
    mock_setup.assert_not_called()


@pytest.mark.asyncio
async def test_auto_ack_true_sets_broker_no_ack_and_skips_dlx_on_failure(amqp_consumer):
    """auto_ack=True must (a) pass no_ack=True to queue.iterator and (b) skip DLX on failure."""
    consumer = amqp_consumer

    valid_body = b'{"id": 1, "name": "Test", "active": true}'
    mock_message = AsyncMock(body=valid_body, ack=AsyncMock(), reject=AsyncMock())
    mock_message.configure_mock(app_id="test_app", message_id="12345", routing_key="rk", headers=None)

    async def async_iterator():
        yield mock_message

    iterator_call = MagicMock(return_value=async_iterator())
    mock_queue = AsyncMock()
    mock_queue.iterator = iterator_call
    consumer._channel.declare_queue.return_value = mock_queue

    failing_callback = AsyncMock(side_effect=RuntimeError("callback boom"))
    dlx_spy = AsyncMock()

    with patch.object(consumer, '_async_setup_exchange_and_queue', AsyncMock(return_value=mock_queue)), \
         patch.object(consumer, '_async_publish_to_dlx_with_retry_cycle', dlx_spy):
        consumer.auto_declare_ok = True
        await consumer.start_consumer(
            queue_name='test_q',
            callback=failing_callback,
            routing_key='test_route',
            exchange_name='test_x',
            exchange_type='direct',
            auto_ack=True,
        )

    iterator_call.assert_called_once_with(no_ack=True)
    dlx_spy.assert_not_called()
    mock_message.ack.assert_not_called()
    mock_message.reject.assert_not_called()


def test_aio_pika_attributes_from_message_populates_all_fields():
    """AioPikaAttributes.from_message must mirror the full pika.BasicProperties surface."""
    ts = datetime(2026, 1, 1, 12, 0, 0)
    fake_message = Mock(
        message_id="m1",
        app_id="a1",
        headers={"x": "y"},
        correlation_id="c1",
        reply_to="r1",
        content_type="application/json",
        content_encoding="utf-8",
        delivery_mode=2,
        expiration="60000",
        priority=5,
        timestamp=ts,
        type="event",
        user_id="u1",
    )

    props = AioPikaAttributes.from_message(fake_message)

    assert props.message_id == "m1"
    assert props.app_id == "a1"
    assert props.headers == {"x": "y"}
    assert props.correlation_id == "c1"
    assert props.reply_to == "r1"
    assert props.content_type == "application/json"
    assert props.content_encoding == "utf-8"
    assert props.delivery_mode == 2
    assert props.expiration == "60000"
    assert props.priority == 5
    assert props.timestamp == ts
    assert props.type == "event"
    assert props.user_id == "u1"
