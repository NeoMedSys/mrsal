import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from mrsal.amqp.subclass import MrsalBlockingAMQP, MrsalAsyncAMQP
from tests.conftest import SETUP_ARGS, ExpectedPayload


class TestDLXConfiguration:
	"""Test Dead Letter Exchange configuration."""
	
	def test_dlx_enabled_by_default(self):
		"""Test that DLX is enabled by default."""
		consumer = MrsalBlockingAMQP(**SETUP_ARGS)
		assert consumer.dlx_enable is True
		
	def test_dlx_can_be_disabled(self):
		"""Test that DLX can be explicitly disabled."""
		consumer = MrsalBlockingAMQP(**SETUP_ARGS, dlx_enable=False)
		assert consumer.dlx_enable is False

	@patch('mrsal.amqp.subclass.MrsalBlockingAMQP.setup_blocking_connection')
	def test_dlx_setup_in_exchange_and_queue(self, mock_setup):
		"""Test that DLX exchange is created during setup."""
		consumer = MrsalBlockingAMQP(**SETUP_ARGS)
		consumer._channel = MagicMock()
		
		# Mock the declare methods
		consumer._declare_exchange = Mock()
		consumer._declare_queue = Mock()
		consumer._declare_queue_binding = Mock()
		
		consumer._setup_exchange_and_queue(
			exchange_name="test_exchange",
			queue_name="test_queue",
			exchange_type="direct",
			routing_key="test_key",
			dlx_enable=True
		)
		
		# Should declare DLX exchange and main exchange
		assert consumer._declare_exchange.call_count == 2
		
		# Check DLX exchange was declared
		dlx_call = consumer._declare_exchange.call_args_list[0]
		assert dlx_call[1]['exchange'] == "test_queue.dlx"
		
	@patch('mrsal.amqp.subclass.MrsalBlockingAMQP.setup_blocking_connection')
	def test_custom_dlx_name(self, mock_setup):
		"""Test custom DLX exchange name."""
		consumer = MrsalBlockingAMQP(**SETUP_ARGS)
		consumer._channel = MagicMock()
		
		consumer._declare_exchange = Mock()
		consumer._declare_queue = Mock()
		consumer._declare_queue_binding = Mock()
		
		consumer._setup_exchange_and_queue(
			exchange_name="test_exchange",
			queue_name="test_queue",
			exchange_type="direct",
			routing_key="test_key",
			dlx_enable=True,
			dlx_exchange_name="custom_dlx"
		)
		
		# Check custom DLX name was used
		dlx_call = consumer._declare_exchange.call_args_list[0]
		assert dlx_call[1]['exchange'] == "custom_dlx"


class TestQuorumQueues:
	"""Test Quorum queue configuration."""
	
	def test_quorum_enabled_by_default(self):
		"""Test that quorum queues are enabled by default."""
		consumer = MrsalBlockingAMQP(**SETUP_ARGS)
		assert consumer.use_quorum_queues is True
		
	@patch('mrsal.amqp.subclass.MrsalBlockingAMQP.setup_blocking_connection')
	def test_quorum_queue_arguments(self, mock_setup):
		"""Test that quorum queue arguments are added."""
		consumer = MrsalBlockingAMQP(**SETUP_ARGS)
		consumer._channel = MagicMock()
		
		consumer._declare_exchange = Mock()
		consumer._declare_queue = Mock()
		consumer._declare_queue_binding = Mock()
		
		consumer._setup_exchange_and_queue(
			exchange_name="test_exchange",
			queue_name="test_queue",
			exchange_type="direct",
			routing_key="test_key",
			use_quorum_queues=True
		)
		
		# Check quorum arguments were added to queue
		queue_call = consumer._declare_queue.call_args
		queue_args = queue_call[1]['arguments']
		
		assert 'x-queue-type' in queue_args
		assert queue_args['x-queue-type'] == 'quorum'
		assert 'x-quorum-initial-group-size' in queue_args


class TestRetryMechanism:
	"""Test message retry mechanism."""
	
	@pytest.fixture
	def mock_consumer(self):
		"""Create a mocked consumer for retry testing."""
		consumer = MrsalBlockingAMQP(**SETUP_ARGS, max_retries=2)  # Low retry count for testing
		consumer._channel = MagicMock()
		consumer.setup_blocking_connection = Mock()
		consumer._setup_exchange_and_queue = Mock()
		consumer.auto_declare_ok = True
		return consumer
	
	def test_max_retries_setting(self):
		"""Test that max_retries can be configured."""
		consumer = MrsalBlockingAMQP(**SETUP_ARGS, max_retries=5)
		assert consumer.max_retries == 5
		
	def test_successful_processing_no_retry(self, mock_consumer):
		"""Test that successful processing doesn't trigger retries."""
		# Mock a valid message
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 123
		mock_properties = MagicMock()
		valid_body = b'{"id": 1, "name": "Test", "active": true}'
		
		mock_consumer._channel.consume.return_value = [(mock_method_frame, mock_properties, valid_body)]
		mock_callback = Mock()
		
		mock_consumer.start_consumer(
			queue_name="test_queue",
			callback=mock_callback,
			auto_ack=False,
			auto_declare=True,
			exchange_name="test_exchange",
			exchange_type="direct",
			routing_key="test_key",
			payload_model=ExpectedPayload
		)
		
		# Should call ack, not nack
		mock_consumer._channel.basic_ack.assert_called_once_with(delivery_tag=123)
		mock_consumer._channel.basic_nack.assert_not_called()
		
	def test_validation_failure_with_dlx(self, mock_consumer):
		"""Test that validation failures eventually go to DLX after max retries."""
		# Mock an invalid message that will fail validation
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 123
		mock_properties = MagicMock()
		invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'
		
		# Need to return the same message multiple times to simulate retries
		mock_consumer._channel.consume.return_value = [
			(mock_method_frame, mock_properties, invalid_body)
		] * 3
		
		mock_consumer.start_consumer(
			queue_name="test_queue",
			callback=Mock(),
			auto_ack=False,
			auto_declare=True,
			exchange_name="test_exchange", 
			exchange_type="direct",
			routing_key="test_key",
			payload_model=ExpectedPayload,
			dlx_enable=True  # DLX enabled
		)
		
		# Should eventually nack with requeue=False (send to DLX)
		final_nack_call = mock_consumer._channel.basic_nack.call_args_list[-1]
		assert final_nack_call[1]['delivery_tag'] == 123
		assert final_nack_call[1]['requeue'] == False  # Goes to DLX
		
	def test_validation_failure_without_dlx(self, mock_consumer):
		"""Test that validation failures are dropped when no DLX configured."""
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 123
		mock_properties = MagicMock()
		invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'
		
		mock_consumer._channel.consume.return_value = [
			(mock_method_frame, mock_properties, invalid_body)
		] * 10
		
		with patch.object(mock_consumer.log, 'warning') as mock_log_warning:
			mock_consumer.start_consumer(
				queue_name="test_queue",
				callback=Mock(),
				auto_ack=False,
				auto_declare=True,
				exchange_name="test_exchange",
				exchange_type="direct", 
				routing_key="test_key",
				payload_model=ExpectedPayload,
				dlx_enable=False  # No DLX
			)
			
			# Should log the expected warning about dropping message
			mock_log_warning.assert_called()
			warning_message = mock_log_warning.call_args[0][0]
			assert "No dead letter exchange declared for" in warning_message
			assert "reflect on your life choices! byebye" in warning_message


class TestIntegration:
	"""Integration tests for the complete flow."""
	
	@patch('mrsal.amqp.subclass.MrsalBlockingAMQP.setup_blocking_connection')
	def test_complete_consumer_setup_with_defaults(self, mock_setup):
		"""Test complete consumer setup with all default features enabled."""
		consumer = MrsalBlockingAMQP(**SETUP_ARGS)
		consumer._channel = MagicMock()
		
		# Mock all the declare methods
		consumer._declare_exchange = Mock()
		consumer._declare_queue = Mock() 
		consumer._declare_queue_binding = Mock()
		
		# Mock consume to return one message then stop
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 789
		mock_properties = MagicMock()
		valid_body = b'{"id": 1, "name": "Test", "active": true}'
		
		mock_consumer_iter = iter([(mock_method_frame, mock_properties, valid_body)])
		consumer._channel.consume.return_value = mock_consumer_iter
		
		mock_callback = Mock()
		
		consumer.start_consumer(
			queue_name="integration_queue",
			callback=mock_callback,
			auto_ack=False,
			auto_declare=True,
			exchange_name="integration_exchange",
			exchange_type="direct",
			routing_key="integration_key"
			# Using all defaults: dlx_enable=True, use_quorum_queues=True, max_retries=3
		)
		
		# Verify all components were set up
		assert consumer._declare_exchange.call_count == 2  # Main + DLX
		assert consumer._declare_queue.call_count == 1
		assert consumer._declare_queue_binding.call_count == 1
		
		# Verify DLX was configured
		dlx_call = consumer._declare_exchange.call_args_list[0]
		assert dlx_call[1]['exchange'] == "integration_queue.dlx"
		
		# Verify quorum queue was configured
		queue_call = consumer._declare_queue.call_args
		queue_args = queue_call[1]['arguments']
		assert queue_args['x-queue-type'] == 'quorum'
		assert 'x-dead-letter-exchange' in queue_args
		
		# Verify message was processed successfully
		mock_callback.assert_called_once()
		consumer._channel.basic_ack.assert_called_once_with(delivery_tag=789)
		
	def test_minimal_setup_all_disabled(self):
		"""Test minimal setup with all enhanced features disabled."""
		consumer = MrsalBlockingAMQP(
			**SETUP_ARGS,
			dlx_enable=False,
			use_quorum_queues=False,
			max_retries=1
		)
		
		# Verify settings
		assert consumer.dlx_enable is False
		assert consumer.use_quorum_queues is False
		assert consumer.max_retries == 1

