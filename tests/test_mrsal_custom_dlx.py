import pytest
from unittest.mock import Mock, MagicMock, AsyncMock, patch
from mrsal.amqp.subclass import MrsalBlockingAMQP, MrsalAsyncAMQP
from pydantic.dataclasses import dataclass

from mrsal.exceptions import MrsalAbortedSetup


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


class TestDLXExchangeNameConfiguration:
	"""Test DLX exchange name configuration and fallback behavior"""

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
			max_retries=2,
			use_quorum_queues=True,
			blocked_connection_timeout=60
		)

		# Mock connection and channel
		consumer._connection = MagicMock()
		consumer._channel = MagicMock()
		consumer.auto_declare_ok = True

		# Mock setup methods
		consumer.setup_blocking_connection = MagicMock()
		consumer._setup_exchange_and_queue = MagicMock()

		return consumer

	@pytest.fixture
	def mock_async_consumer(self):
		"""Create a mock async consumer"""
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
			max_retries=2,
			use_quorum_queues=True
		)

		# Mock connection and channel
		consumer._connection = AsyncMock()
		consumer._channel = AsyncMock()
		consumer.auto_declare_ok = True

		# Mock setup methods
		consumer.setup_async_connection = AsyncMock()
		consumer._async_setup_exchange_and_queue = AsyncMock()

		return consumer

	def test_dlx_fallback_naming_sync(self, mock_consumer):
		"""Test that DLX exchange name falls back to {exchange}.dlx when not specified"""
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 123
		mock_method_frame.routing_key = "test_key"
		mock_properties = MagicMock()
		mock_properties.message_id = 'test_msg'
		mock_properties.app_id = 'test_app'
		mock_properties.headers = None
		invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'

		def debug_consume_generator():
			print("DEBUG: Starting consume generator")
			for i in range(3):
				print(f"DEBUG: Yielding message {i+1}")
				yield (mock_method_frame, mock_properties, invalid_body)
			print("DEBUG: Generator exhausted")

		# Create generator that simulates redelivery cycle
		def consume_generator():
			for _ in range(3):	# Max retries + 1
				yield (mock_method_frame, mock_properties, invalid_body)

		mock_consumer._channel.consume.return_value = consume_generator()

		# Mock the DLX publishing method to capture calls
		mock_consumer._publish_to_dlx = MagicMock()

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
			# dlx_exchange_name NOT specified - should use fallback
			retry_cycle_interval=10,
			max_retry_time_limit=60
		)

		# Verify _publish_to_dlx was called with fallback name "test_exchange.dlx"
		mock_consumer._publish_to_dlx.assert_called()
		call_args = mock_consumer._publish_to_dlx.call_args
		dlx_exchange_used = call_args[0][0]  # First positional argument
		assert dlx_exchange_used == "test_exchange.dlx"

	def test_dlx_custom_naming_sync(self, mock_consumer):
		"""Test that custom DLX exchange name is used when specified"""
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 123
		mock_method_frame.routing_key = "test_key"
		mock_properties = MagicMock()
		mock_properties.message_id = 'test_msg'
		mock_properties.app_id = 'test_app'
		mock_properties.headers = None
		invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'

		def consume_generator():
			for _ in range(3):
				yield (mock_method_frame, mock_properties, invalid_body)

		mock_consumer._channel.consume.return_value = consume_generator()
		mock_consumer._publish_to_dlx = MagicMock()

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
			dlx_exchange_name="custom_dlx_exchange",  # Custom DLX name
			retry_cycle_interval=10,
			max_retry_time_limit=60
		)

		# Verify _publish_to_dlx was called with custom name
		mock_consumer._publish_to_dlx.assert_called()
		call_args = mock_consumer._publish_to_dlx.call_args
		print(f"DEBUG - All call args: {call_args}")
		print(f"DEBUG - Positional args: {call_args[0] if call_args[0] else 'None'}")

		dlx_exchange_used = call_args[0][0]
		assert dlx_exchange_used == "custom_dlx_exchange"

	@pytest.mark.asyncio
	async def test_dlx_fallback_naming_async(self, mock_async_consumer):
		"""Test async DLX exchange name fallback behavior"""
		mock_message = MagicMock()
		mock_message.delivery_tag = 123
		mock_message.app_id = 'test_app'
		mock_message.headers = None
		mock_message.body = b'{"id": "wrong_type", "name": "Test", "active": true}'
		mock_message.ack = AsyncMock()
		mock_message.reject = AsyncMock()

		mock_properties = MagicMock()
		mock_properties.headers = None

		mock_queue = AsyncMock()
		mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue

		# Multiple redeliveries to trigger DLX
		async_iterator = AsyncIteratorMock([mock_message] * 3)
		mock_queue.iterator = Mock(return_value=async_iterator)

		# Mock the DLX publishing method
		mock_async_consumer._publish_to_dlx = AsyncMock()

		await mock_async_consumer.start_consumer(
			queue_name="test_queue",
			callback=AsyncMock(side_effect=Exception("Processing failed")),
			auto_ack=False,
			auto_declare=True,
			exchange_name="async_exchange",
			exchange_type="direct",
			routing_key="test_key",
			payload_model=ExpectedPayload,
			dlx_enable=True,
			enable_retry_cycles=True,
			# dlx_exchange_name NOT specified - should use fallback
			retry_cycle_interval=10,
			max_retry_time_limit=60
		)

		# Verify _publish_to_dlx was called with fallback name
		mock_async_consumer._publish_to_dlx.assert_called()
		print(f"DEBUG: _publish_to_dlx call_args: {mock_async_consumer._publish_to_dlx.call_args}")
		print(f"DEBUG: All calls: {mock_async_consumer._publish_to_dlx.call_args_list}")
		call_args = mock_async_consumer._publish_to_dlx.call_args
		dlx_exchange_used = call_args[0][0]
		assert dlx_exchange_used == "async_exchange.dlx"

	@pytest.mark.asyncio
	async def test_dlx_custom_naming_async(self, mock_async_consumer):
		"""Test async custom DLX exchange name usage"""
		mock_message = MagicMock()
		mock_message.delivery_tag = 123
		mock_message.app_id = 'test_app'
		mock_message.headers = None
		mock_message.body = b'{"id": "wrong_type", "name": "Test", "active": true}'
		mock_message.ack = AsyncMock()
		mock_message.reject = AsyncMock()

		mock_properties = MagicMock()
		mock_properties.headers = None

		mock_queue = AsyncMock()
		mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue

		async_iterator = AsyncIteratorMock([mock_message] * 3)
		mock_queue.iterator = Mock(return_value=async_iterator)

		mock_async_consumer._publish_to_dlx = AsyncMock()

		await mock_async_consumer.start_consumer(
			queue_name="test_queue",
			callback=AsyncMock(side_effect=Exception("Processing failed")),
			auto_ack=False,
			auto_declare=True,
			exchange_name="async_exchange",
			exchange_type="direct",
			routing_key="test_key",
			payload_model=ExpectedPayload,
			dlx_enable=True,
			enable_retry_cycles=True,
			dlx_exchange_name="async_custom_dlx",  # Custom DLX name
			retry_cycle_interval=10,
			max_retry_time_limit=60
		)

		# Verify custom DLX name was used
		mock_async_consumer._publish_to_dlx.assert_called()
		call_args = mock_async_consumer._publish_to_dlx.call_args
		dlx_exchange_used = call_args[0][0]
		assert dlx_exchange_used == "async_custom_dlx"

	def test_dlx_exchange_setup_fallback_sync(self, mock_consumer):
		"""Test that DLX exchange setup uses fallback naming"""
		mock_consumer.start_consumer(
			queue_name="setup_test_queue",
			auto_declare=True,
			exchange_name="setup_exchange",
			exchange_type="direct",
			routing_key="setup_key",
			dlx_enable=True,
			# dlx_exchange_name not specified
		)

		# Verify _setup_exchange_and_queue was called
		mock_consumer._setup_exchange_and_queue.assert_called()
		call_args = mock_consumer._setup_exchange_and_queue.call_args[1]  # kwargs

		# Check that dlx_exchange_name parameter is None (will use fallback)
		assert call_args.get('dlx_exchange_name') is None
		assert call_args.get('exchange_name') == "setup_exchange"

	def test_dlx_exchange_setup_custom_sync(self, mock_consumer):
		"""Test that DLX exchange setup uses custom naming when provided"""
		mock_consumer.start_consumer(
			queue_name="setup_test_queue",
			auto_declare=True,
			exchange_name="setup_exchange",
			exchange_type="direct",
			routing_key="setup_key",
			dlx_enable=True,
			dlx_exchange_name="custom_setup_dlx"
		)

		mock_consumer._setup_exchange_and_queue.assert_called()
		call_args = mock_consumer._setup_exchange_and_queue.call_args[1]

		# Check that custom dlx_exchange_name was passed
		assert call_args.get('dlx_exchange_name') == "custom_setup_dlx"

	@pytest.mark.asyncio
	async def test_dlx_exchange_setup_fallback_async(self, mock_async_consumer):
		"""Test async DLX exchange setup with fallback naming"""
		mock_queue = AsyncMock()
		mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue

		async_iterator = AsyncIteratorMock([])	# Empty to avoid processing
		mock_queue.iterator = Mock(return_value=async_iterator)

		await mock_async_consumer.start_consumer(
			queue_name="async_setup_queue",
			auto_declare=True,
			exchange_name="async_setup_exchange",
			exchange_type="direct",
			routing_key="async_setup_key",
			dlx_enable=True,
			# dlx_exchange_name not specified
		)

		mock_async_consumer._async_setup_exchange_and_queue.assert_called()
		call_args = mock_async_consumer._async_setup_exchange_and_queue.call_args[1]

		assert call_args.get('dlx_exchange_name') is None
		assert call_args.get('exchange_name') == "async_setup_exchange"

	@pytest.mark.asyncio
	async def test_dlx_exchange_setup_custom_async(self, mock_async_consumer):
		"""Test async DLX exchange setup with custom naming"""
		mock_queue = AsyncMock()
		mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue

		async_iterator = AsyncIteratorMock([])
		mock_queue.iterator = Mock(return_value=async_iterator)

		await mock_async_consumer.start_consumer(
			queue_name="async_setup_queue",
			auto_declare=True,
			exchange_name="async_setup_exchange",
			exchange_type="direct",
			routing_key="async_setup_key",
			dlx_enable=True,
			dlx_exchange_name="async_custom_setup_dlx"
		)

		mock_async_consumer._async_setup_exchange_and_queue.assert_called()
		call_args = mock_async_consumer._async_setup_exchange_and_queue.call_args[1]

		assert call_args.get('dlx_exchange_name') == "async_custom_setup_dlx"
