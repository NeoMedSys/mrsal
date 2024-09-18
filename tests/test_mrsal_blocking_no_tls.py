import pytest
from unittest.mock import Mock, patch, MagicMock, call
from pika.exceptions import AMQPConnectionError, UnroutableError
from pydantic.dataclasses import dataclass
from tenacity import RetryError
from mrsal.amqp.subclass import MrsalBlockingAMQP

# Configuration and expected payload definition
SETUP_ARGS = {
    'host': 'localhost',
    'port': 5672,
    'credentials': ('user', 'password'),
    'virtual_host': 'testboi',
    'ssl': False,
    'heartbeat': 60,
    'blocked_connection_timeout': 60,
    'prefetch_count': 1
}

@dataclass
class ExpectedPayload:
    id: int
    name: str
    active: bool


# Fixture to mock the BlockingConnection and the setup connection method
@pytest.fixture
def mock_amqp_connection():
    with patch('mrsal.amqp.subclass.pika.BlockingConnection') as mock_blocking_connection, \
         patch('mrsal.amqp.subclass.MrsalBlockingAMQP.setup_blocking_connection', autospec=True) as mock_setup_blocking_connection:
        
        # Set up the mock behaviors for the connection and channel
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_blocking_connection.return_value = mock_connection
        
        # Ensure setup_blocking_connection does nothing during the tests
        mock_setup_blocking_connection.return_value = None
        
        # Provide the mocks for use in the test
        yield mock_connection, mock_channel, mock_setup_blocking_connection


# Fixture to create a MrsalBlockingAMQP consumer with mocked channel
@pytest.fixture
def amqp_consumer(mock_amqp_connection):
    mock_connection, mock_channel, _ = mock_amqp_connection
    consumer = MrsalBlockingAMQP(**SETUP_ARGS)
    consumer._channel = mock_channel  # Inject the mocked channel into the consumer
    return consumer


def test_retry_on_connection_failure_blocking(amqp_consumer, mock_amqp_connection):
    """Test reconnection retries in blocking consumer mode."""
    mock_connection, mock_channel, mock_setup_blocking_connection = mock_amqp_connection
    mock_channel.consume.side_effect = AMQPConnectionError("Connection lost")

    # Attempt to start the consumer, which should trigger the retry
    with pytest.raises(RetryError):
        amqp_consumer.start_consumer(
            queue_name='test_q',
            exchange_name='test_x',
            exchange_type='direct',
            routing_key='test_route',
            callback=Mock(),
        )

    # Verify that setup_blocking_connection was retried 3 times
    assert mock_setup_blocking_connection.call_count == 3


def test_valid_message_processing(amqp_consumer):
    # Simulate a valid message
    valid_body = b'{"id": 1, "name": "Test", "active": true}'
    mock_method_frame = MagicMock()
    mock_properties = MagicMock()

    # Mock the consume method to yield a valid message
    amqp_consumer._channel.consume.return_value = [(mock_method_frame, mock_properties, valid_body)]

    mock_callback = Mock()

    amqp_consumer.start_consumer(
        queue_name='test_q',
        exchange_name='test_x',
        exchange_type='direct',
        routing_key='test_route',
        callback=mock_callback,
        payload_model=ExpectedPayload
    )

    # Assert the callback was called once with the correct data
    mock_callback.assert_called_once_with(mock_method_frame, mock_properties, valid_body)


def test_valid_message_processing_no_autoack(amqp_consumer):
    """Test that a message is acknowledged on successful processing."""
    valid_body = b'{"id": 1, "name": "Test", "active": true}'
    mock_method_frame = MagicMock()
    mock_method_frame.delivery_tag = 123
    mock_properties = MagicMock()

    amqp_consumer._channel.consume.return_value = [(mock_method_frame, mock_properties, valid_body)]
    mock_callback = Mock()

    amqp_consumer.start_consumer(
        exchange_name="test_x",
        exchange_type="direct",
        queue_name="test_q",
        routing_key="test_route",
        callback=mock_callback,
        payload_model=ExpectedPayload,
        auto_ack=False
    )

    amqp_consumer._channel.basic_ack.assert_called_once_with(delivery_tag=123)


def test_invalid_message_skipped(amqp_consumer):
    # Simulate an invalid message that fails validation
    invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'
    mock_method_frame = MagicMock()
    mock_properties = MagicMock()

    # Mock the consume method to yield an invalid message
    amqp_consumer._channel.consume.return_value = [(mock_method_frame, mock_properties, invalid_body)]

    mock_callback = Mock()

    amqp_consumer.start_consumer(
        queue_name='test_queue',
        auto_ack=True,
        exchange_name='test_x',
        exchange_type='direct',
        routing_key='test_route',
        callback=mock_callback,
        payload_model=ExpectedPayload
    )

    # Assert the callback was not called since the message should be skipped
    mock_callback.assert_not_called()


def test_requeue_on_validation_failure(amqp_consumer):
    # Simulate an invalid message that fails validation
    invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'
    mock_method_frame = MagicMock()
    mock_method_frame.delivery_tag = 123
    mock_properties = MagicMock()

    # Mock the consume method to yield an invalid message
    amqp_consumer._channel.consume.return_value = [(mock_method_frame, mock_properties, invalid_body)]

    with patch.object(amqp_consumer._channel, 'basic_nack') as mock_nack:
        amqp_consumer.start_consumer(
            queue_name='test_q',
            auto_ack=False,
            exchange_name='test_x',
            exchange_type='direct',
            routing_key='test_route',
            payload_model=ExpectedPayload
        )

        # Assert that basic_nack was called with requeue=True
        mock_nack.assert_called_once_with(delivery_tag=123, requeue=True)


def test_publish_message(amqp_consumer):
    """Test that the message is correctly published to the exchange."""
    amqp_consumer._setup_exchange_and_queue = Mock()

    message = b'{"data": "test_message"}'
    exchange_name = 'test_x'
    routing_key = 'test_route'

    amqp_consumer.publish_message(
        exchange_name=exchange_name,
        routing_key=routing_key,
        message=message,
        exchange_type='direct',
        queue_name='test_q',
        auto_declare=True
    )

    amqp_consumer._setup_exchange_and_queue.assert_called_once_with(
        exchange_name=exchange_name,
        queue_name='test_q',
        exchange_type='direct',
        routing_key=routing_key
    )

    amqp_consumer._channel.basic_publish.assert_called_once_with(
        exchange=exchange_name,
        routing_key=routing_key,
        body=message,
        properties=None
    )


def test_retry_on_unroutable_error(amqp_consumer):
    """Test that the publish_message retries 3 times when UnroutableError is raised."""
    amqp_consumer._setup_exchange_and_queue = Mock()

    message = "test_message"
    exchange_name = 'test_x'
    routing_key = 'test_route'
    queue_name = 'test_q'

    amqp_consumer._channel.basic_publish.side_effect = UnroutableError("Message could not be routed")

    with pytest.raises(RetryError):
        amqp_consumer.publish_message(
            exchange_name=exchange_name,
            routing_key=routing_key,
            message=message,
            exchange_type='direct',
            queue_name=queue_name,
            auto_declare=True
        )

    assert amqp_consumer._channel.basic_publish.call_count == 3

    expected_call = call(
        exchange=exchange_name,
        routing_key=routing_key,
        body=message,
        properties=None
    )
    amqp_consumer._channel.basic_publish.assert_has_calls([expected_call] * 3)
