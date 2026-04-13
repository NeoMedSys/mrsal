import pytest
from unittest.mock import Mock, patch, MagicMock, call, ANY
from pika.exceptions import AMQPConnectionError, UnroutableError
from pydantic.dataclasses import dataclass
from tenacity import RetryError, stop_after_attempt
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
    consumer._connection = mock_connection
    consumer._channel = mock_channel  # Inject the mocked channel into the consumer
    consumer._consumer_channel = mock_channel
    mock_connection.is_open = False
    mock_connection.channel.return_value = mock_channel
    return consumer


def test_retry_on_connection_failure_blocking(amqp_consumer, mock_amqp_connection):
    """Test that blocking consumer retries on connection failure with exponential backoff."""
    mock_connection, mock_channel, mock_setup_blocking_connection = mock_amqp_connection
    mock_channel.consume.side_effect = AMQPConnectionError("Connection lost")

    # Temporarily cap retries so the test terminates
    original_retry = amqp_consumer.start_consumer.retry
    amqp_consumer.start_consumer.retry.stop = stop_after_attempt(3)

    try:
        with pytest.raises(RetryError):
            amqp_consumer.start_consumer(
                queue_name='test_q',
                exchange_name='test_x',
                exchange_type='direct',
                routing_key='test_route',
                callback=Mock(),
            )

        # Verify retries happened
        assert mock_setup_blocking_connection.call_count == 3
    finally:
        amqp_consumer.start_consumer.retry.stop = original_retry.stop


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
        routing_key=routing_key,
        passive=True,
        channel=ANY
    )

    amqp_consumer._channel.basic_publish.assert_called_once_with(
        exchange=exchange_name,
        routing_key=routing_key,
        body=message,
        properties=None
    )


def test_publish_messages(amqp_consumer):
    """Test that the message is correctly published to the exchange."""
    amqp_consumer._setup_exchange_and_queue = Mock()

    mrsal_protocol = {
            'app1': {
                'message': b'{"data": "king"}',
                'routing_key': 'king_route',
                'queue_name': 'queue-king',
                'exchange_type': 'topic',
                'exchange_name': 'exchange.king'
                },

            'app2': {
                'message': b'{"data": "unga"}',
                'routing_key': 'unga_route',
                'queue_name': 'queue-unga',
                'exchange_type': 'topic',
                'exchange_name': 'exchange.unga'
                }
            }

    amqp_consumer.publish_messages(
            mrsal_protocol_collection=mrsal_protocol
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


def test_publish_from_inside_consumer_callback(mock_amqp_connection):
    """Regression: publishing from inside a consumer callback must not kill the consumer.

    Before the fix, publish_message() called setup_blocking_connection() which replaced
    self._connection and self._channel, destroying the channel the consumer loop was
    iterating on. The consumer's next iteration would crash with StreamLostError.
    """
    mock_connection, mock_channel, _ = mock_amqp_connection

    # Create separate mock channels for consumer vs publisher so we can verify isolation
    consumer_channel = MagicMock(name='consumer_channel')
    publisher_channel = MagicMock(name='publisher_channel')
    mock_connection.channel.side_effect = [consumer_channel, publisher_channel]
    mock_connection.is_open = False  # force _ensure_connection to call setup_blocking_connection

    consumer = MrsalBlockingAMQP(**SETUP_ARGS)
    consumer._connection = mock_connection
    consumer._setup_exchange_and_queue = Mock()

    # Simulate two messages in the consume loop
    msg1_frame = MagicMock()
    msg1_frame.delivery_tag = 1
    msg1_props = MagicMock()
    msg1_props.headers = None
    msg2_frame = MagicMock()
    msg2_frame.delivery_tag = 2
    msg2_props = MagicMock()
    msg2_props.headers = None
    body = b'{"data": "test"}'

    consumer_channel.consume.return_value = [
        (msg1_frame, msg1_props, body),
        (msg2_frame, msg2_props, body),
    ]

    callback_calls = []

    def callback(method_frame, properties, body):
        callback_calls.append(method_frame.delivery_tag)
        # Publish from inside the callback — this is the pattern that used to crash
        consumer.publish_message(
            exchange_name='downstream',
            routing_key='downstream',
            message=b'result',
            exchange_type='direct',
            queue_name='downstream',
            auto_declare=False
        )

    consumer.start_consumer(
        queue_name='test_q',
        exchange_name='test_x',
        exchange_type='direct',
        routing_key='test_route',
        callback=callback,
        auto_declare=False,
        auto_ack=True,
    )

    # Both messages were processed — consumer survived the mid-callback publish
    assert callback_calls == [1, 2]

    # Publisher used its own channel, not the consumer channel
    publisher_channel.basic_publish.assert_called()
    consumer_channel.basic_publish.assert_not_called()


def test_ephemeral_channel_closed_after_publish(mock_amqp_connection):
    """The ephemeral channel created by publish_message must be closed in finally."""
    mock_connection, mock_channel, _ = mock_amqp_connection
    mock_connection.is_open = True
    mock_connection.channel.return_value = mock_channel

    consumer = MrsalBlockingAMQP(**SETUP_ARGS)
    consumer._connection = mock_connection
    consumer._setup_exchange_and_queue = Mock()

    consumer.publish_message(
        exchange_name='test_x',
        routing_key='test_route',
        message=b'msg',
        exchange_type='direct',
        queue_name='test_q',
        auto_declare=False
    )

    mock_channel.close.assert_called_once()


def test_ephemeral_channel_closed_on_publish_error(mock_amqp_connection):
    """The ephemeral channel must be closed even when basic_publish raises."""
    mock_connection, mock_channel, _ = mock_amqp_connection
    mock_connection.is_open = True
    mock_connection.channel.return_value = mock_channel

    consumer = MrsalBlockingAMQP(**SETUP_ARGS)
    consumer._connection = mock_connection
    consumer._setup_exchange_and_queue = Mock()
    mock_channel.basic_publish.side_effect = Exception("unexpected")

    consumer.publish_message(
        exchange_name='test_x',
        routing_key='test_route',
        message=b'msg',
        exchange_type='direct',
        queue_name='test_q',
        auto_declare=False
    )

    # Channel closed despite the error (caught by except Exception in publish_message)
    mock_channel.close.assert_called_once()


def test_declare_methods_use_passed_channel():
    """Superclass declare methods must use the channel parameter when provided."""
    from mrsal.superclass import Mrsal

    consumer = MrsalBlockingAMQP(**SETUP_ARGS)
    consumer._channel = MagicMock(name='default_channel')
    explicit_channel = MagicMock(name='explicit_channel')

    # _declare_exchange should use explicit_channel, not self._channel
    consumer._declare_exchange(
        exchange='test_x',
        exchange_type='direct',
        arguments=None,
        durable=True,
        passive=False,
        internal=False,
        auto_delete=False,
        channel=explicit_channel
    )
    explicit_channel.exchange_declare.assert_called_once()
    consumer._channel.exchange_declare.assert_not_called()

    # _declare_queue should use explicit_channel
    consumer._declare_queue(
        queue='test_q',
        arguments=None,
        durable=True,
        exclusive=False,
        auto_delete=False,
        passive=False,
        channel=explicit_channel
    )
    explicit_channel.queue_declare.assert_called_once()
    consumer._channel.queue_declare.assert_not_called()

    # _declare_queue_binding should use explicit_channel
    consumer._declare_queue_binding(
        exchange='test_x',
        queue='test_q',
        routing_key='test_route',
        arguments=None,
        channel=explicit_channel
    )
    explicit_channel.queue_bind.assert_called_once()
    consumer._channel.queue_bind.assert_not_called()


def test_declare_methods_fallback_to_self_channel():
    """Superclass declare methods must fall back to self._channel when channel=None."""
    consumer = MrsalBlockingAMQP(**SETUP_ARGS)
    consumer._channel = MagicMock(name='default_channel')

    consumer._declare_exchange(
        exchange='test_x',
        exchange_type='direct',
        arguments=None,
        durable=True,
        passive=False,
        internal=False,
        auto_delete=False
    )
    consumer._channel.exchange_declare.assert_called_once()


def test_connection_reuse_across_publishes(mock_amqp_connection):
    """Publishing N times should reuse one connection, not open N connections."""
    mock_connection, mock_channel, mock_setup = mock_amqp_connection

    consumer = MrsalBlockingAMQP(**SETUP_ARGS)
    consumer._connection = mock_connection
    consumer._connection.is_open = True  # connection is alive
    consumer._setup_exchange_and_queue = Mock()
    mock_connection.channel.return_value = mock_channel

    for _ in range(10):
        consumer.publish_message(
            exchange_name='test_x',
            routing_key='test_route',
            message=b'msg',
            exchange_type='direct',
            queue_name='test_q',
            auto_declare=False
        )

    # setup_blocking_connection should NOT have been called — connection was already open
    mock_setup.assert_not_called()
    # basic_publish was called 10 times
    assert mock_channel.basic_publish.call_count == 10


def test_setup_blocking_connection_reraises_unexpected_exception():
    """Unexpected exceptions from setup_blocking_connection must propagate, not be swallowed."""
    consumer = MrsalBlockingAMQP(**SETUP_ARGS)

    with patch('mrsal.amqp.subclass.pika.BlockingConnection',
               side_effect=RuntimeError("disk on fire")):
        with pytest.raises(RuntimeError, match="disk on fire"):
            consumer.setup_blocking_connection()
