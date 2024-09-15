import os
import unittest
from unittest.mock import Mock, patch, MagicMock, call
from pika.exceptions import AMQPConnectionError, UnroutableError
from pydantic import ValidationError

from mrsal.amqp.baseclasses import MrsalAMQP
from tenacity import RetryError
from tests.conftest import SETUP_ARGS, ExpectedPayload



class TestMrsalBlockingAMQP(unittest.TestCase):
    @patch('mrsal.amqp.baseclasses.MrsalAMQP.setup_blocking_connection')
    @patch('mrsal.amqp.baseclasses.pika.channel')
    def setUp(self, mock_blocking_connection, mock_setup_connection):
        # Set up mock behaviors for the connection and channel
        self.mock_channel = MagicMock()
        self.mock_connection = MagicMock()
        self.mock_connection.channel.return_value = self.mock_channel
        mock_blocking_connection.return_value = self.mock_connection

        # Mock the setup_connection to simulate a successful connection setup
        mock_setup_connection.return_value = None  # Simulate setup_connection doing nothing (successful setup)

        # Create an instance of BlockRabbit
        self.consumer = MrsalAMQP(**SETUP_ARGS, use_blocking=True)
        self.consumer._channel = self.mock_channel  # Set the channel to the mocked one

    @patch.object(MrsalAMQP, 'setup_blocking_connection')
    def test_retry_on_connection_failure_blocking(self, mock_blocking_connection):
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
                    callback=mock_callback,
                    )

        self.assertEqual(mock_blocking_connection.call_count, 3)

    def test_valid_message_processing(self):
        # Simulate a valid message
        valid_body = b'{"id": 1, "name": "Test", "active": true}'
        mock_method_frame = MagicMock()
        mock_properties = MagicMock()

        # Mock the consume method to yield a valid message
        self.mock_channel.consume.return_value = [(mock_method_frame, mock_properties, valid_body)]

        # Set up a mock callback function
        mock_callback = Mock()


        # Start the consumer with the payload model and callback
        self.consumer.start_consumer(
            queue_name='test_q',
            exchange_name='test_x',
            exchange_type='direct',
            routing_key='test_route',
            callback=mock_callback,
            payload_model=ExpectedPayload
        )

        # Assert the callback was called once with the correct data
        mock_callback.assert_called_once_with(mock_method_frame, mock_properties, valid_body)

    def test_valid_message_processing_no_autoack(self):
        """Test that a message is acknowledged on successful processing."""
        valid_body = b'{"id": 1, "name": "Test", "active": true}'
        mock_method_frame = MagicMock()
        mock_method_frame.delivery_tag = 123
        mock_properties = MagicMock()

        self.mock_channel.consume.return_value = [(mock_method_frame, mock_properties, valid_body)]
        mock_callback = Mock()

        self.consumer.start_consumer(
            exchange_name="test_x",
            exchange_type="direct",
            queue_name="test_q",
            routing_key="test_route",
            callback=mock_callback,
            payload_model=ExpectedPayload,
            auto_ack=False
        )

        self.mock_channel.basic_ack.assert_called_once_with(delivery_tag=123)

    def test_invalid_message_skipped(self):
        # Simulate an invalid message that fails validation
        invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'
        mock_method_frame = MagicMock()
        mock_properties = MagicMock()

        # Mock the consume method to yield an invalid message
        self.mock_channel.consume.return_value = [(mock_method_frame, mock_properties, invalid_body)]

        # Set up a mock callback function
        mock_callback = Mock()

        # Start the consumer with the payload model and callback
        self.consumer.start_consumer(
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

    def test_requeue_on_validation_failure(self):
        # Simulate an invalid message that fails validation
        invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'
        mock_method_frame = MagicMock()
        mock_method_frame.delivery_tag = 123  # Set a delivery tag for nack
        mock_properties = MagicMock()

        # Mock the consume method to yield an invalid message
        self.mock_channel.consume.return_value = [(mock_method_frame, mock_properties, invalid_body)]

        # Start the consumer with the payload model
        with patch.object(self.consumer._channel, 'basic_nack') as mock_nack:
            self.consumer.start_consumer(
                queue_name='test_q',
                auto_ack=False,  # Disable auto_ack to test nack behavior
                exchange_name='test_x',
                exchange_type='direct',
                routing_key='test_route',
                payload_model=ExpectedPayload
            )

            # Assert that basic_nack was called with requeue=True
            mock_nack.assert_called_once_with(delivery_tag=123, requeue=True)

    def test_publish_message(self):
        """Test that the message is correctly published to the exchange."""
        # Mock the setup methods for auto declare
        self.consumer._setup_exchange_and_queue = Mock()

        # Mock the message to be published
        message = b'{"data": "test_message"}'
        exchange_name = 'test_x'
        routing_key = 'test_route'

        # Publish the message
        self.consumer.publish_message(
            exchange_name=exchange_name,
            routing_key=routing_key,
            message=message,
            exchange_type='direct',
            queue_name='test_q',
            auto_declare=True
        )

        # Assert the setup was called
        self.consumer._setup_exchange_and_queue.assert_called_once_with(
            exchange_name=exchange_name,
            queue_name='test_q',
            exchange_type='direct',
            routing_key=routing_key
        )

        # Assert the message was published correctly
        self.mock_channel.basic_publish.assert_called_once_with(
            exchange=exchange_name,
            routing_key=routing_key,
            body=message,
            properties=None
        )

    def test_retry_on_unroutable_error(self):
        """Test that the publish_message retries 3 times when UnroutableError is raised."""
        # Mock the setup methods for auto declare
        self.consumer._setup_exchange_and_queue = Mock()

        # Set up the message and parameters
        message = "test_message"
        exchange_name = 'test_x'
        routing_key = 'test_route'
        queue_name = 'test_q'

        # Mock the basic_publish to raise UnroutableError
        self.mock_channel.basic_publish.side_effect = UnroutableError("Message could not be routed")

        # Atempt to publish the message
        with self.assertRaises(RetryError):
            self.consumer.publish_message(
                exchange_name=exchange_name,
                routing_key=routing_key,
                message=message,
                exchange_type='direct',
                queue_name=queue_name,
                auto_declare=True
            )

        # Assert that basic_publish was called 3 times due to retries
        self.assertEqual(self.mock_channel.basic_publish.call_count, 3)
        # Assert that "test_message" appears 3 times

        # Assert the correct calls were made with the expected arguments
        expected_call = call(
            exchange=exchange_name,
            routing_key=routing_key,
            body=message,
            properties=None
        )
        self.mock_channel.basic_publish.assert_has_calls([expected_call] * 3)

class TestBlockRabbitSSLSetup(unittest.TestCase):

    def test_ssl_setup_with_valid_paths(self):
        with patch.dict('os.environ', {
            'RABBITMQ_CERT': 'test_cert.crt',
            'RABBITMQ_KEY': 'test_key.key',
            'RABBITMQ_CAFILE': 'test_ca.ca'
        }, clear=True):
            consumer = MrsalAMQP(**SETUP_ARGS, ssl=True, use_blocking=True)

            # Check if SSL paths are correctly loaded and blocking is used
            self.assertEqual(consumer.tls_dict['crt'], 'test_cert.crt')
            self.assertEqual(consumer.tls_dict['key'], 'test_key.key')
            self.assertEqual(consumer.tls_dict['ca'], 'test_ca.ca')

    @patch.dict('os.environ', {
        'RABBITMQ_CERT': '',
        'RABBITMQ_KEY': '',
        'RABBITMQ_CAFILE': ''
    })
    def test_ssl_setup_with_missing_paths(self):
        with self.assertRaises(ValidationError):
            MrsalAMQP(**SETUP_ARGS, ssl=True, use_blocking=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_ssl_setup_without_env_vars(self):
        with self.assertRaises(ValidationError):
            MrsalAMQP(**SETUP_ARGS, ssl=True, use_blocking=True)


if __name__ == '__main__':
    unittest.main()
