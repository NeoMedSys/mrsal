import unittest
from unittest.mock import Mock, MagicMock, patch
from mrsal.amqp.subclass import MrsalAMQP
from mrsal.exceptions import MrsalAbortedSetup, MrsalSetupError
from pika.exceptions import AMQPConnectionError
from tenacity import RetryError
from tests.conftest import SETUP_ARGS, ExpectedPayload

class TestMrsalAsyncAMQP(unittest.TestCase):
    def setUp(self):
        self.mock_channel = MagicMock()
        self.consumer = MrsalAMQP(**SETUP_ARGS)
        self.consumer._channel = self.mock_channel

    @patch.object(MrsalAMQP, 'setup_async_connection')
    def test_retry_on_connection_failure_blocking(self, mock_async_connection):
        """Test reconnection retries in blocking consumer mode."""

        # Set up a mock callback function
        mock_callback = Mock()

        self.mock_channel.consume.side_effect = AMQPConnectionError("Connection lost")

        with self.assertRaises(RetryError):
            self.consumer.start_consumer(
                    queue_name='test_q',
                    exchange_name='test_x',
                    exchange_type='direct',
                    routing_key='test_route',
                    callback=mock_callback
                    )

        self.assertEqual(mock_async_connection.call_count, 3)

    @patch('mrsal.amqp.subclass.MrsalAMQP._setup_exchange_and_queue')
    def test_raises_mrsal_aborted_setup_on_failed_auto_declaration(self, mock_setup_exchange_and_queue):
        """Test that MrsalAbortedSetup is raised if the auto declaration fails."""
        self.consumer.auto_declare_ok = False  # Simulate auto declaration failure
        mock_setup_exchange_and_queue.return_value = None  # Simulate the method execution without error
        with self.assertRaises(MrsalAbortedSetup):
            self.consumer.start_consumer(
                exchange_name="test_exchange",
                exchange_type="direct",
                queue_name="test_queue",
                routing_key="test_route"
            )

    def test_setup_raises_setup_error_on_exchange_failure(self):
        """Test that MrsalSetupError is raised if exchange declaration fails."""
        self.mock_channel.exchange_declare.side_effect = MrsalSetupError("Exchange error")
        with self.assertRaises(MrsalSetupError):
            self.consumer._declare_exchange(
                exchange="test_x",
                exchange_type="direct",
                arguments=None,
                durable=True,
                passive=False,
                internal=False,
                auto_delete=False
            )

    def test_setup_raises_setup_error_on_queue_failure(self):
        """Test that MrsalSetupError is raised if queue declaration fails."""
        self.mock_channel.queue_declare.side_effect = MrsalSetupError("Queue error")
        with self.assertRaises(MrsalSetupError):
            self.consumer._declare_queue(
                queue="test_q",
                arguments=None,
                durable=True,
                exclusive=False,
                auto_delete=False,
                passive=False
                )

    def test_setup_raises_setup_error_on_binding_failure(self):
        """Test that MrsalSetupError is raised if queue binding fails."""
        self.mock_channel.queue_bind.side_effect = MrsalSetupError("Bind error")
        with self.assertRaises(MrsalSetupError):
            self.consumer._declare_queue_binding(
                    exchange='test_x',
                    queue='test_q',
                    routing_key='test_route',
                    arguments=None
            )

    def test_valid_message_processing(self):
        """Test message processing with a valid payload and a user-defined callback."""
        valid_body = b'{"id": 1, "name": "Test", "active": true}'
        mock_method_frame = MagicMock()
        mock_properties = MagicMock()

        self.mock_channel.consume.return_value = [(mock_method_frame, mock_properties, valid_body)]
        mock_callback = Mock()

        self.consumer.start_consumer(
            exchange_name="test_exchange",
            exchange_type="direct",
            queue_name="test_queue",
            routing_key="test_route",
            callback=mock_callback,
            payload_model=ExpectedPayload
        )

        mock_callback.assert_called_once_with(mock_method_frame, mock_properties, valid_body)

    def test_invalid_message_skips_processing(self):
        """Test that invalid payloads are skipped and do not invoke the callback."""
        invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'
        mock_method_frame = MagicMock()
        mock_properties = MagicMock()

        self.mock_channel.consume.return_value = [(mock_method_frame, mock_properties, invalid_body)]
        mock_callback = Mock()

        self.consumer.start_consumer(
            exchange_name="test_exchange",
            exchange_type="direct",
            queue_name="test_queue",
            routing_key="test_route",
            callback=mock_callback,
            payload_model=ExpectedPayload
        )

        mock_callback.assert_not_called()

    def test_message_acknowledgment_on_success(self):
        """Test that a message is acknowledged on successful processing."""
        valid_body = b'{"id": 1, "name": "Test", "active": true}'
        mock_method_frame = MagicMock()
        mock_method_frame.delivery_tag = 123
        mock_properties = MagicMock()

        self.mock_channel.consume.return_value = [(mock_method_frame, mock_properties, valid_body)]
        mock_callback = Mock()

        self.consumer.start_consumer(
            exchange_name="test_exchange",
            exchange_type="direct",
            queue_name="test_queue",
            routing_key="test_route",
            callback=mock_callback,
            payload_model=ExpectedPayload,
            auto_ack=False
        )

        self.mock_channel.basic_ack.assert_called_once_with(delivery_tag=123)

    def test_message_nack_on_callback_failure(self):
        """Test that a message is nacked and requeued on callback failure."""
        valid_body = b'{"id": 1, "name": "Test", "active": true}'
        mock_method_frame = MagicMock()
        mock_method_frame.delivery_tag = 123
        mock_properties = MagicMock()

        self.mock_channel.consume.return_value = [(mock_method_frame, mock_properties, valid_body)]
        mock_callback = Mock(side_effect=Exception("Callback error"))

        self.consumer.start_consumer(
            exchange_name="test_exchange",
            exchange_type="direct",
            queue_name="test_queue",
            routing_key="test_route",
            callback=mock_callback,
            payload_model=ExpectedPayload,
            auto_ack=False
        )

        self.mock_channel.basic_nack.assert_called_once_with(delivery_tag=123, requeue=True)


if __name__ == '__main__':
    unittest.main()
