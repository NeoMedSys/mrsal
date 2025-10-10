import pytest
from unittest.mock import Mock, MagicMock, AsyncMock, patch
from pydantic.dataclasses import dataclass

from mrsal.amqp.subclass import MrsalBlockingAMQP, MrsalAsyncAMQP


@dataclass
class ExpectedPayload:
	id: int
	name: str
	active: bool


class AsyncIteratorMock:
	"""Mock async iterator for aio-pika queue.iterator()"""
	def __init__(self, items):
		self.items = iter(items)

	def __aiter__(self):
		return self

	async def __anext__(self):
		try:
			return next(self.items)
		except StopIteration:
			raise StopAsyncIteration


class TestDLXRetryCycleOnly:
	"""Test DLX retry cycle mechanism WITHOUT immediate retries"""

	@pytest.fixture
	def mock_consumer(self):
		"""Create a mock consumer with mocked connection and channel"""
		consumer = MrsalBlockingAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			ssl=False,
			verbose=False,
			prefetch_count=1,
			heartbeat=60,
			dlx_enable=True,
			use_quorum_queues=True,
			blocked_connection_timeout=60
		)

		consumer._connection = MagicMock()
		consumer._channel = MagicMock()
		consumer.auto_declare_ok = True
		consumer.setup_blocking_connection = MagicMock()
		consumer._setup_exchange_and_queue = MagicMock()

		return consumer

	def test_validation_failure_sends_to_dlx_immediately(self, mock_consumer):
		"""
		Test that validation failures send message directly to DLX
		NO immediate retries - goes straight to DLX
		"""
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 456
		mock_method_frame.routing_key = "test_key"
		mock_method_frame.exchange = "test_exchange"

		invalid_body = b'{"id": "not_an_int", "name": "Test", "active": true}'

		props = MagicMock()
		props.message_id = 'validation_test'
		props.app_id = 'test_app'
		props.headers = None

		mock_consumer._channel.consume.return_value = [
			(mock_method_frame, props, invalid_body)
		]

		mock_consumer._publish_to_dlx_with_retry_cycle = MagicMock()

		mock_consumer.start_consumer(
			queue_name="test_queue",
			callback=Mock(),
			auto_ack=False,
			auto_declare=True,
			exchange_name="test_exchange",
			exchange_type="direct",
			routing_key="test_key",
			payload_model=ExpectedPayload,
			dlx_enable=True,
			enable_retry_cycles=True,
			retry_cycle_interval=10,
			max_retry_time_limit=60
		)

		# Should NOT nack with requeue
		mock_consumer._channel.basic_nack.assert_not_called()

		# Should call DLX immediately (no immediate retries)
		mock_consumer._publish_to_dlx_with_retry_cycle.assert_called_once()
		
		# Verify the call had correct parameters
		call_args = mock_consumer._publish_to_dlx_with_retry_cycle.call_args
		assert call_args[0][0] == mock_method_frame
		assert call_args[0][1] == props
		assert call_args[0][2] == invalid_body

	def test_callback_failure_sends_to_dlx_with_cycles_enabled(self, mock_consumer):
		"""
		Test callback failure with retry_cycles enabled.
		Should send to DLX immediately for retry cycle
		"""
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 789
		mock_method_frame.routing_key = "test_key"
		mock_method_frame.exchange = "test_exchange"

		valid_body = b'{"id": 123, "name": "Test", "active": true}'

		props = MagicMock()
		props.message_id = 'callback_fail'
		props.app_id = 'test_app'
		props.headers = None

		mock_consumer._channel.consume.return_value = [
			(mock_method_frame, props, valid_body)
		]

		# Callback that always fails
		failing_callback = Mock(side_effect=Exception("Processing failed"))
		mock_consumer._publish_to_dlx_with_retry_cycle = MagicMock()

		mock_consumer.start_consumer(
			queue_name="test_queue",
			callback=failing_callback,
			auto_ack=False,
			auto_declare=True,
			exchange_name="test_exchange",
			exchange_type="direct",
			routing_key="test_key",
			payload_model=ExpectedPayload,
			dlx_enable=True,
			enable_retry_cycles=True,
			retry_cycle_interval=10,
			max_retry_time_limit=60
		)

		# Callback should be called once (no immediate retries)
		assert failing_callback.call_count == 1

		# Should send to DLX with retry cycle
		mock_consumer._publish_to_dlx_with_retry_cycle.assert_called_once()

	def test_callback_failure_with_retry_cycles_disabled(self, mock_consumer):
		"""
		Test callback failure with retry_cycles disabled.
		Should nack to DLX without retry cycle logic
		"""
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 999
		mock_method_frame.routing_key = "test_key"

		valid_body = b'{"id": 123, "name": "Test", "active": true}'

		props = MagicMock()
		props.message_id = 'callback_no_cycle'
		props.app_id = 'test_app'
		props.headers = None

		mock_consumer._channel.consume.return_value = [
			(mock_method_frame, props, valid_body)
		]

		failing_callback = Mock(side_effect=Exception("Processing failed"))

		mock_consumer.start_consumer(
			queue_name="test_queue",
			callback=failing_callback,
			auto_ack=False,
			auto_declare=True,
			exchange_name="test_exchange",
			exchange_type="direct",
			routing_key="test_key",
			payload_model=ExpectedPayload,
			dlx_enable=True,
			enable_retry_cycles=False  # Disabled
		)

		# Should nack with requeue=False (goes to DLX)
		nack_calls = mock_consumer._channel.basic_nack.call_args_list
		assert len(nack_calls) == 1
		assert nack_calls[0][1]['delivery_tag'] == 999
		assert nack_calls[0][1]['requeue'] == False

	def test_successful_processing_acks_message(self, mock_consumer):
		"""Test that successful processing acks immediately without retries"""
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 111
		
		valid_body = b'{"id": 456, "name": "Success", "active": false}'

		props = MagicMock()
		props.message_id = 'success_msg'
		props.app_id = 'test_app'
		props.headers = None

		mock_consumer._channel.consume.return_value = [
			(mock_method_frame, props, valid_body)
		]

		successful_callback = Mock()  # No exception

		mock_consumer.start_consumer(
			queue_name="test_queue",
			callback=successful_callback,
			auto_ack=False,
			auto_declare=True,
			exchange_name="test_exchange",
			exchange_type="direct",
			routing_key="test_key",
			payload_model=ExpectedPayload
		)

		# Should ack immediately
		mock_consumer._channel.basic_ack.assert_called_once_with(delivery_tag=111)

		# Should NOT nack or send to DLX
		mock_consumer._channel.basic_nack.assert_not_called()

	def test_no_dlx_drops_message(self, mock_consumer):
		"""Test that without DLX, failed messages are dropped"""
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 222

		invalid_body = b'{"id": "bad", "name": "Test", "active": true}'

		props = MagicMock()
		props.message_id = 'no_dlx'
		props.app_id = 'test_app'
		props.headers = None

		mock_consumer._channel.consume.return_value = [
			(mock_method_frame, props, invalid_body)
		]

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

		# Should nack with requeue=False (message dropped)
		nack_calls = mock_consumer._channel.basic_nack.call_args_list
		assert len(nack_calls) == 1
		assert nack_calls[0][1]['requeue'] == False


class TestAsyncDLXRetryCycleOnly:
	"""Test async consumer with DLX retry cycles (no immediate retries)"""

	@pytest.fixture
	def mock_async_consumer(self):
		consumer = MrsalAsyncAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			ssl=False,
			verbose=False,
			prefetch_count=1,
			heartbeat=60,
			dlx_enable=True,
			use_quorum_queues=True
		)

		consumer._connection = AsyncMock()
		consumer._channel = AsyncMock()
		consumer.auto_declare_ok = True
		consumer.setup_async_connection = AsyncMock()
		consumer._async_setup_exchange_and_queue = AsyncMock()

		return consumer

	@pytest.mark.asyncio
	async def test_async_validation_failure_sends_to_dlx(self, mock_async_consumer):
		"""Test async validation failure sends to DLX immediately"""
		invalid_body = b'{"id": "wrong", "name": "Test", "active": true}'

		mock_msg = MagicMock()
		mock_msg.delivery_tag = 123
		mock_msg.app_id = 'async_app'
		mock_msg.headers = None
		mock_msg.body = invalid_body
		mock_msg.reject = AsyncMock()
		mock_msg.ack = AsyncMock()

		mock_properties = MagicMock()
		mock_properties.headers = None

		mock_queue = AsyncMock()
		mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue

		async_iterator = AsyncIteratorMock([mock_msg])
		mock_queue.iterator = Mock(return_value=async_iterator)

		mock_async_consumer._publish_to_dlx = AsyncMock()

		await mock_async_consumer.start_consumer(
			queue_name="test_queue",
			callback=AsyncMock(),
			auto_ack=False,
			auto_declare=True,
			exchange_name="test_exchange",
			exchange_type="direct",
			routing_key="test_key",
			payload_model=ExpectedPayload,
			enable_retry_cycles=True,
			retry_cycle_interval=10,
			max_retry_time_limit=60
		)

		# Should NOT reject (handled by DLX publish)
		mock_msg.reject.assert_not_called()

		# Should call DLX
		mock_async_consumer._publish_to_dlx.assert_called_once()

	@pytest.mark.asyncio
	async def test_async_successful_processing(self, mock_async_consumer):
		"""Test async successful message processing"""
		valid_body = b'{"id": 789, "name": "AsyncSuccess", "active": true}'

		mock_msg = MagicMock()
		mock_msg.delivery_tag = 456
		mock_msg.app_id = 'async_app'
		mock_msg.headers = None
		mock_msg.body = valid_body
		mock_msg.ack = AsyncMock()
		mock_msg.reject = AsyncMock()

		mock_properties = MagicMock()
		mock_properties.headers = None

		mock_queue = AsyncMock()
		mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue

		async_iterator = AsyncIteratorMock([mock_msg])
		mock_queue.iterator = Mock(return_value=async_iterator)

		successful_callback = AsyncMock()

		await mock_async_consumer.start_consumer(
			queue_name="test_queue",
			callback=successful_callback,
			auto_ack=False,
			auto_declare=True,
			exchange_name="test_exchange",
			exchange_type="direct",
			routing_key="test_key",
			payload_model=ExpectedPayload
		)

		# Should ack
		mock_msg.ack.assert_called_once()

		# Should NOT reject
		mock_msg.reject.assert_not_called()


class TestDLXRetryHeaders:
	"""Test DLX retry cycle header management"""

	@pytest.fixture
	def mock_consumer(self):
		consumer = MrsalBlockingAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			dlx_enable=True
		)
		consumer._connection = MagicMock()
		consumer._channel = MagicMock()
		return consumer

	def test_retry_cycle_info_extraction(self, mock_consumer):
		"""Test extraction of retry cycle information from headers"""
		mock_properties = MagicMock()
		mock_properties.headers = {
			'x-cycle-count': 2,
			'x-first-failure': '2024-01-01T10:00:00Z',
			'x-total-elapsed': 120000
		}

		retry_info = mock_consumer._get_retry_cycle_info(mock_properties)

		assert retry_info['cycle_count'] == 2
		assert retry_info['first_failure'] == '2024-01-01T10:00:00Z'
		assert retry_info['total_elapsed'] == 120000

	def test_should_continue_retry_cycles_time_limit(self, mock_consumer):
		"""Test retry cycle time limit checking"""
		# Within time limit
		retry_info = {'total_elapsed': 30000}
		should_continue = mock_consumer._should_continue_retry_cycles(
			retry_info, enable_retry_cycles=True, max_retry_time_limit=1
		)
		assert should_continue is True

		# Exceeded time limit
		retry_info = {'total_elapsed': 120000}
		should_continue = mock_consumer._should_continue_retry_cycles(
			retry_info, enable_retry_cycles=True, max_retry_time_limit=1
		)
		assert should_continue is False
