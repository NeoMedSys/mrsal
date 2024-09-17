from mrsal.exceptions import MrsalAbortedSetup, MrsalSetupError
import pytest
from unittest.mock import Mock, patch, MagicMock, call
from pika.exceptions import AMQPConnectionError, UnroutableError
from pydantic.dataclasses import dataclass
from tenacity import RetryError
from mrsal.amqp.subclass import MrsalAMQP
from pika.adapters.asyncio_connection import AsyncioConnection

# Configuration and expected payload definition
SETUP_ARGS = {
    'host': 'localhost',
    'port': 5672,
    'credentials': ('user', 'password'),
    'virtual_host': 'testboi',
    'ssl': False,
    'use_blocking': False,
    'heartbeat': 60,
    'blocked_connection_timeout': 60,
    'prefetch_count': 1
}

@dataclass
class ExpectedPayload:
    id: int
    name: str
    active: bool


# Fixture to mock the AsyncioConnection and the setup connection method
@pytest.fixture
def mock_async_amqp_connection():
    with patch('mrsal.amqp.subclass.pika.adapters.asyncio_connection.AsyncioConnection') as mock_async_connection, \
         patch('mrsal.amqp.subclass.MrsalAMQP.setup_async_connection', autospec=True) as mock_setup_async_connection:
        
        # Set up the mock behaviors for the connection and channel
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_async_connection.return_value = mock_connection
        
        # Ensure setup_async_connection does nothing during the tests
        mock_setup_async_connection.return_value = None
        
        # Provide the mocks for use in the test
        yield mock_connection, mock_channel, mock_setup_async_connection


# Fixture to create a MrsalAMQP consumer with mocked channel for async mode
@pytest.fixture
def async_amqp_consumer(mock_async_amqp_connection):
    mock_connection, mock_channel, _ = mock_async_amqp_connection
    consumer = MrsalAMQP(**SETUP_ARGS)
    consumer._channel = mock_channel  # Inject the mocked channel into the consumer
    consumer._connection = mock_connection  # Inject the mocked async connection
    return consumer


def test_retry_on_connection_failure_async(async_amqp_consumer, mock_async_amqp_connection):
    """Test reconnection retries in async consumer mode."""
    mock_connection, mock_channel, mock_setup_async_connection = mock_async_amqp_connection
    mock_channel.consume.side_effect = AMQPConnectionError("Connection lost")

    # Attempt to start the consumer, which should trigger the retry
    with pytest.raises(RetryError):
        async_amqp_consumer.start_consumer(
            queue_name='test_q',
            exchange_name='test_x',
            exchange_type='direct',
            routing_key='test_route',
            callback=Mock(),
        )

    # Verify that setup_async_connection was retried 3 times
    assert mock_setup_async_connection.call_count == 3


def test_valid_message_processing_async(async_amqp_consumer):
    """Test message processing with a valid payload in async mode."""
    valid_body = b'{"id": 1, "name": "Test", "active": true}'
    mock_method_frame = MagicMock()
    mock_properties = MagicMock()

    # Mock the consume method to yield a valid message
    async_amqp_consumer._channel.consume.return_value = [(mock_method_frame, mock_properties, valid_body), (None, None, None)]

    mock_callback = Mock()

    try:
        async_amqp_consumer.start_consumer(
            queue_name='test_q',
            exchange_name='test_x',
            exchange_type='direct',
            routing_key='test_route',
            callback=mock_callback,
            payload_model=ExpectedPayload
        )
    except Exception:
        print("Controlled exit of run_forever")

    # Assert the callback was called once with the correct data
    mock_callback.assert_called_once_with(mock_method_frame, mock_properties, valid_body)


def test_valid_message_processing_no_autoack_async(async_amqp_consumer):
    """Test that a message is acknowledged on successful processing in async mode."""
    valid_body = b'{"id": 1, "name": "Test", "active": true}'
    mock_method_frame = MagicMock()
    mock_method_frame.delivery_tag = 123
    mock_properties = MagicMock()

    async_amqp_consumer._channel.consume.return_value = [(mock_method_frame, mock_properties, valid_body)]
    mock_callback = Mock()

    try:
        async_amqp_consumer.start_consumer(
            exchange_name="test_x",
            exchange_type="direct",
            queue_name="test_q",
            routing_key="test_route",
            callback=mock_callback,
            payload_model=ExpectedPayload,
            auto_ack=False
        )
    except Exception:
        print("Controlled exit of run_forever")

    async_amqp_consumer._channel.basic_ack.assert_called_once_with(delivery_tag=123)


def test_invalid_message_skipped_async(async_amqp_consumer):
    """Test that invalid payloads are skipped in async mode."""
    invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'
    mock_method_frame = MagicMock()
    mock_properties = MagicMock()

    # Mock the consume method to yield an invalid message
    async_amqp_consumer._channel.consume.return_value = [(mock_method_frame, mock_properties, invalid_body), (None, None, None)]

    mock_callback = Mock()

    try:
        async_amqp_consumer.start_consumer(
            queue_name='test_queue',
            auto_ack=True,
            exchange_name='test_x',
            exchange_type='direct',
            routing_key='test_route',
            callback=mock_callback,
            payload_model=ExpectedPayload
        )
    except Exception:
        print("Controlled exit of run_forever")

    # Assert the callback was not called since the message should be skipped
    mock_callback.assert_not_called()


def test_requeue_on_validation_failure_async(async_amqp_consumer):
    """Test that a message is requeued on validation failure in async mode."""
    invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'
    mock_method_frame = MagicMock()
    mock_method_frame.delivery_tag = 123
    mock_properties = MagicMock()

    # Mock the consume method to yield an invalid message
    async_amqp_consumer._channel.consume.return_value = [(mock_method_frame, mock_properties, invalid_body), (None, None, None)]

    with patch.object(async_amqp_consumer._channel, 'basic_nack') as mock_nack:
        try:
            async_amqp_consumer.start_consumer(
                queue_name='test_q',
                auto_ack=False,
                exchange_name='test_x',
                exchange_type='direct',
                routing_key='test_route',
                payload_model=ExpectedPayload
            )
        except Exception:
            print("Controlled exit of run_forever")

        # Assert that basic_nack was called with requeue=True
        mock_nack.assert_called_once_with(delivery_tag=123, requeue=True)


def test_raises_mrsal_aborted_setup_on_failed_auto_declaration(async_amqp_consumer):
    """Test that MrsalAbortedSetup is raised if the auto declaration fails."""
    async_amqp_consumer.auto_declare_ok = False  # Simulate auto declaration failure

    # Mock `_setup_exchange_and_queue` to simulate setup failure without raising MrsalSetupError directly
    with patch.object(MrsalAMQP, '_setup_exchange_and_queue') as mock_setup_exchange_and_queue, \
         patch.object(async_amqp_consumer._connection, 'ioloop') as mock_ioloop, \
         patch.object(mock_ioloop, 'run_forever', return_value=None) as mock_run_forever, \
         patch.object(mock_ioloop, 'stop', return_value=None) as mock_stop:

        # Simulate `_setup_exchange_and_queue` setting `auto_declare_ok` to False
        mock_setup_exchange_and_queue.side_effect = lambda *args, **kwargs: setattr(async_amqp_consumer, 'auto_declare_ok', False)

        # The intention here is to trigger MrsalAbortedSetup due to failed auto declaration
        with pytest.raises(MrsalAbortedSetup):
            async_amqp_consumer.start_consumer(
                exchange_name="test_exchange",
                exchange_type="direct",
                queue_name="test_queue",
                routing_key="test_route"
            )

        # Assert that the ioloop stop was called to ensure the connection handling behavior
        mock_stop.assert_called_once()
        mock_run_forever.assert_called_once()
