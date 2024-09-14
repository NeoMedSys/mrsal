import os
import unittest
from unittest.mock import Mock, patch, MagicMock
from pydantic import ValidationError

from mrsal.amqp.baseclasses import MrsalBlockingAMQP
from tests.conftest import SETUP_ARGS, ExpectedPayload



class TestMrsalBlockingAMQP(unittest.TestCase):
    @patch('mrsal.amqp.baseclasses.MrsalBlockingAMQP.setup_connection')
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
        self.consumer = MrsalBlockingAMQP(**SETUP_ARGS)
        self.consumer._channel = self.mock_channel  # Set the channel to the mocked one

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

   # def test_requeue_on_validation_failure(self):
   #     # Simulate an invalid message that fails validation
   #     invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'
   #     mock_method_frame = MagicMock()
   #     mock_method_frame.delivery_tag = 123  # Set a delivery tag for nack
   #     mock_properties = MagicMock()

   #     # Mock the consume method to yield an invalid message
   #     self.mock_channel.consume.return_value = [(mock_method_frame, mock_properties, invalid_body)]

   #     # Start the consumer with the payload model
   #     with patch.object(self.consumer._channel, 'basic_nack') as mock_nack:
   #         self.consumer.start_consumer(
   #             queue='test_queue',
   #             auto_ack=False,  # Disable auto_ack to test nack behavior
                # exchange_name='test_x',
                #    exchange_type='direct',
                #    routing_key='test_route',
   #             payload_model=.ExpectedPayload
   #         )

   #         # Assert that basic_nack was called with requeue=True
   #         mock_nack.assert_called_once_with(delivery_tag=123, requeue=True)

class TestBlockRabbitSSLSetup(unittest.TestCase):

    def test_ssl_setup_with_valid_paths(self):
        with patch.dict('os.environ', {
            'RABBITMQ_CERT': 'test_cert.crt',
            'RABBITMQ_KEY': 'test_key.key',
            'RABBITMQ_CAFILE': 'test_ca.ca'
        }, clear=True):
            consumer = MrsalBlockingAMQP(**SETUP_ARGS, ssl=True)

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
            MrsalBlockingAMQP(**SETUP_ARGS, ssl=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_ssl_setup_without_env_vars(self):
        with self.assertRaises(ValidationError):
            MrsalBlockingAMQP(**SETUP_ARGS, ssl=True)


if __name__ == '__main__':
    unittest.main()
