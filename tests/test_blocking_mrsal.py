import unittest
from unittest.mock import Mock, patch, MagicMock
from pydantic.dataclasses import dataclass
from pydantic import ValidationError
from typing import Any

from mrsal.amqp.baseclasses import MrsalBlockingAMQP, MrsalAsyncAMQP

@dataclass
class ExpectedPayload:
    id: int
    name: str
    active: bool

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
        self.consumer = MrsalBlockingAMQP()
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
            queue='test_queue',
            auto_ack=True,
            callback=mock_callback,
            payload_model=ExpectedPayload
        )

        # Assert the callback was called once with the correct data
        mock_callback.assert_called_once_with(mock_method_frame, mock_properties, valid_body)

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
            queue='test_queue',
            auto_ack=True,
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
                queue='test_queue',
                auto_ack=False,  # Disable auto_ack to test nack behavior
                payload_model=ExpectedPayload
            )

            # Assert that basic_nack was called with requeue=True
            mock_nack.assert_called_once_with(delivery_tag=123, requeue=True)

if __name__ == '__main__':
    unittest.main()
