import pytest
from unittest.mock import Mock, MagicMock, AsyncMock, patch
from aio_pika.exceptions import DeliveryError

from mrsal.amqp.subclass import MrsalBlockingAMQP, MrsalAsyncAMQP
from mrsal.exceptions import MrsalAbortedSetup
from tests.conftest import AsyncIteratorMock, ExpectedPayload


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
		consumer._consumer_channel = consumer._channel
		consumer._connection.channel.return_value = consumer._channel
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


class TestDLXUsesConsumerChannel:
	"""Test that DLX publish and ack/nack operations use _consumer_channel, not _channel."""

	@pytest.fixture
	def mock_consumer(self):
		consumer = MrsalBlockingAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			ssl=False,
			prefetch_count=1,
			heartbeat=60,
			dlx_enable=True,
			blocked_connection_timeout=60
		)

		consumer._connection = MagicMock()
		consumer._channel = MagicMock(name='main_channel')
		consumer._consumer_channel = MagicMock(name='consumer_channel')
		return consumer

	def test_publish_to_dlx_uses_dedicated_channel_with_confirms(self, mock_consumer):
		"""_publish_to_dlx must publish via a dedicated channel with confirm_delivery() enabled,
		not via _consumer_channel — confirms surface broker rejection as an exception."""
		mock_consumer._publish_to_dlx(
			dlx_exchange="test.dlx",
			routing_key="test_key",
			body=b'{"data": "failed"}',
			properties={'headers': {'x-cycle-count': 1}, 'delivery_mode': 2}
		)

		# A dedicated channel was opened and confirm_delivery() called on it
		mock_consumer._connection.channel.assert_called_once_with()
		dlx_channel = mock_consumer._connection.channel.return_value
		dlx_channel.confirm_delivery.assert_called_once()

		# Publish went via the dedicated DLX channel
		dlx_channel.basic_publish.assert_called_once()
		mock_consumer._consumer_channel.basic_publish.assert_not_called()
		mock_consumer._channel.basic_publish.assert_not_called()

	def test_publish_to_dlx_with_retry_cycle_acks_on_consumer_channel(self, mock_consumer):
		"""_publish_to_dlx_with_retry_cycle must ack on _consumer_channel after DLX publish."""
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 42

		mock_properties = MagicMock()
		mock_properties.headers = None
		mock_properties.content_type = 'application/json'

		mock_consumer._publish_to_dlx_with_retry_cycle(
			method_frame=mock_method_frame,
			properties=mock_properties,
			body=b'{"data": "failed"}',
			processing_error="test error",
			original_exchange="test_exchange",
			original_routing_key="test_key",
			enable_retry_cycles=True,
			retry_cycle_interval=10,
			max_retry_time_limit=60,
			dlx_exchange_name=None
		)

		# Ack goes through _consumer_channel
		mock_consumer._consumer_channel.basic_ack.assert_called_once_with(delivery_tag=42)
		mock_consumer._channel.basic_ack.assert_not_called()

	def test_publish_to_dlx_with_retry_cycle_nacks_on_failure(self, mock_consumer):
		"""On DLX publish failure, nack must go through _consumer_channel."""
		mock_method_frame = MagicMock()
		mock_method_frame.delivery_tag = 99

		mock_properties = MagicMock()
		mock_properties.headers = None
		mock_properties.content_type = 'application/json'

		# Make the DLX publish fail on the dedicated DLX channel (publisher-confirm NACK)
		dlx_channel = MagicMock(name='dlx_publish_channel')
		dlx_channel.basic_publish.side_effect = Exception("broker down")
		mock_consumer._connection.channel.return_value = dlx_channel

		mock_consumer._publish_to_dlx_with_retry_cycle(
			method_frame=mock_method_frame,
			properties=mock_properties,
			body=b'{"data": "failed"}',
			processing_error="test error",
			original_exchange="test_exchange",
			original_routing_key="test_key",
			enable_retry_cycles=True,
			retry_cycle_interval=10,
			max_retry_time_limit=60,
			dlx_exchange_name=None
		)

		# Nack without requeue goes through _consumer_channel (requeue=False prevents infinite loops)
		mock_consumer._consumer_channel.basic_nack.assert_called_once_with(
			delivery_tag=99, requeue=False
		)
		mock_consumer._channel.basic_nack.assert_not_called()


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
		consumer._connection.is_closed = False
		consumer._channel = AsyncMock()
		consumer._channel.is_closed = False
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
		mock_msg.message_id = 'msg_123'
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
		mock_msg.message_id = 'msg_456'
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

	@pytest.mark.asyncio
	async def test_async_dlx_publish_failure_rejects_original_message(self, mock_async_consumer):
		"""Broker rejects DLX publish (publisher-confirm NACK / connection loss)
		=> original message must be rejected, not acked."""
		invalid_body = b'{"id": "wrong", "name": "Test", "active": true}'

		mock_msg = MagicMock()
		mock_msg.delivery_tag = 999
		mock_msg.app_id = 'async_app'
		mock_msg.message_id = 'msg_999'
		mock_msg.headers = None
		mock_msg.body = invalid_body
		mock_msg.routing_key = 'test_key'
		mock_msg.ack = AsyncMock()
		mock_msg.reject = AsyncMock()

		mock_queue = AsyncMock()
		mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue

		async_iterator = AsyncIteratorMock([mock_msg])
		mock_queue.iterator = Mock(return_value=async_iterator)

		# Simulate broker rejecting the DLX publish (publisher-confirm NACK).
		mock_async_consumer._publish_to_dlx = AsyncMock(
			side_effect=DeliveryError(None, None)
		)

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

		mock_msg.ack.assert_not_called()
		mock_msg.reject.assert_called_once_with(requeue=False)

	@pytest.mark.asyncio
	async def test_async_dlx_publish_channel_opens_with_publisher_confirms(self, mock_async_consumer):
		"""_publish_to_dlx must open a dedicated channel with publisher_confirms=True
		so broker rejections raise instead of silently dropping the message."""
		dlx_channel = AsyncMock()
		dlx_channel.is_closed = False
		dlx_exchange = AsyncMock()
		dlx_channel.get_exchange = AsyncMock(return_value=dlx_exchange)

		mock_async_consumer._connection.channel = AsyncMock(return_value=dlx_channel)
		mock_async_consumer._dlx_publish_channel = None

		await mock_async_consumer._publish_to_dlx(
			dlx_exchange="dlx",
			routing_key="rk",
			body=b"{}",
			properties={}
		)

		mock_async_consumer._connection.channel.assert_called_once_with(publisher_confirms=True)
		dlx_channel.get_exchange.assert_awaited_once_with("dlx")
		dlx_exchange.publish.assert_awaited_once()
		assert mock_async_consumer._dlx_publish_channel is dlx_channel


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
		consumer._consumer_channel = consumer._channel
		consumer._connection.channel.return_value = consumer._channel
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

	def test_cycling_publish_targets_retry_binding_fixed_mode(self, mock_consumer):
		"""Cycling DLX publish must route to the ``<dlx_routing>.retry`` binding
		so the broker-side TTL on the ``.retry`` queue drives the delay.
		Regression for the silent-drop bug (#84): the previous implementation
		published to the terminal ``.dlx`` binding with a per-message TTL,
		which RabbitMQ then expired without a dead-letter target. In fixed
		mode no per-message ``expiration`` is set -- the queue's
		``x-message-ttl`` provides the wait."""
		mock_properties = MagicMock()
		mock_properties.headers = None
		mock_properties.content_type = 'application/json'

		target_exchange, target_routing, dlx_properties, _, should_cycle, next_delay_ms = mock_consumer._build_dlx_retry_properties(
			properties=mock_properties,
			processing_error="boom",
			original_exchange="orders_exchange",
			original_routing_key="new_order",
			enable_retry_cycles=True,
			retry_cycle_interval=1,
			max_retry_time_limit=60,
			dlx_exchange_name=None,
			retry_backoff="fixed",
		)

		assert target_exchange == "orders_exchange.dlx"
		assert target_routing == "new_order.retry"
		assert should_cycle is True
		assert next_delay_ms is None
		# Fixed mode: queue-level TTL drives the delay, no per-message expiration.
		assert 'expiration_ms' not in dlx_properties
		# Routing-back-to-original lives in queue args, not headers.
		assert 'x-dead-letter-exchange' not in dlx_properties['headers']
		assert 'x-dead-letter-routing-key' not in dlx_properties['headers']
		# Retry tracking headers are still produced.
		assert dlx_properties['headers']['x-cycle-count'] == 1
		assert dlx_properties['headers']['x-retry-exhausted'] is False

	def test_exhausted_publish_targets_terminal_dlx_binding(self, mock_consumer):
		"""When the retry budget is spent, publish must target the terminal
		``.dlx`` binding (not the ``.retry`` one) so the message stays parked
		for human review instead of cycling forever."""
		mock_properties = MagicMock()
		# Already exceeded max_retry_time_limit=1 minute.
		mock_properties.headers = {
			'x-cycle-count': 5,
			'x-first-failure': '2024-01-01T10:00:00Z',
			'x-total-elapsed': 120000
		}
		mock_properties.content_type = 'application/json'

		target_exchange, target_routing, dlx_properties, _, should_cycle, next_delay_ms = mock_consumer._build_dlx_retry_properties(
			properties=mock_properties,
			processing_error="boom",
			original_exchange="orders_exchange",
			original_routing_key="new_order",
			enable_retry_cycles=True,
			retry_cycle_interval=1,
			max_retry_time_limit=1,
			dlx_exchange_name=None,
		)

		assert target_exchange == "orders_exchange.dlx"
		assert target_routing == "new_order"
		assert should_cycle is False
		assert next_delay_ms is None
		assert 'expiration_ms' not in dlx_properties
		assert dlx_properties['headers']['x-retry-exhausted'] is True

	def test_retry_queue_declared_with_ttl_and_dead_letter_args_fixed_mode(self, mock_consumer):
		"""Setup must declare ``<queue>.retry`` with queue-level TTL + DLX args
		pointing back to the original exchange. RabbitMQ does not honor those
		keys in message headers, only in queue arguments.

		In fixed mode the TTL equals ``retry_cycle_interval``."""
		# Capture queue declarations made during setup.
		declared_queues: list[dict] = []
		mock_consumer._declare_exchange = MagicMock()
		mock_consumer._declare_queue = MagicMock(
			side_effect=lambda **kwargs: declared_queues.append(kwargs)
		)
		mock_consumer._declare_queue_binding = MagicMock()

		mock_consumer._setup_exchange_and_queue(
			exchange_name="orders_exchange",
			queue_name="orders_queue",
			exchange_type="direct",
			routing_key="new_order",
			dlx_enable=True,
			enable_retry_cycles=True,
			retry_cycle_interval=5,
			use_quorum_queues=False,
			retry_backoff="fixed",
		)

		retry_decls = [q for q in declared_queues if q['queue'] == 'orders_queue.retry']
		assert len(retry_decls) == 1, f"expected one .retry queue declaration, got {declared_queues}"
		retry_args = retry_decls[0]['arguments']
		assert retry_args['x-message-ttl'] == 5 * 60 * 1000
		assert retry_args['x-dead-letter-exchange'] == 'orders_exchange'
		assert retry_args['x-dead-letter-routing-key'] == 'new_order'

	def test_retry_queue_not_declared_when_retry_cycles_disabled(self, mock_consumer):
		"""Operators who opt out of retry cycles should not get an unused
		``.retry`` queue created on the broker."""
		declared_queues: list[dict] = []
		mock_consumer._declare_exchange = MagicMock()
		mock_consumer._declare_queue = MagicMock(
			side_effect=lambda **kwargs: declared_queues.append(kwargs)
		)
		mock_consumer._declare_queue_binding = MagicMock()

		mock_consumer._setup_exchange_and_queue(
			exchange_name="orders_exchange",
			queue_name="orders_queue",
			exchange_type="direct",
			routing_key="new_order",
			dlx_enable=True,
			enable_retry_cycles=False,
			use_quorum_queues=False,
		)

		retry_decls = [q for q in declared_queues if q['queue'].endswith('.retry')]
		assert retry_decls == []


class TestExponentialBackoff:
	"""Exponential backoff for DLX retry cycles (default mode).

	The .retry queue is declared with x-message-ttl = retry_backoff_max
	(the cap), and each republish stamps a per-message ``expiration`` (ms
	as a string) computed from cycle_count. RabbitMQ honors the shorter
	of queue-TTL vs message-TTL, so the per-message value always wins."""

	@pytest.fixture
	def mock_consumer(self):
		consumer = MrsalBlockingAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			dlx_enable=True,
		)
		consumer._connection = MagicMock()
		consumer._channel = MagicMock()
		consumer._consumer_channel = consumer._channel
		consumer._connection.channel.return_value = consumer._channel
		return consumer

	def test_compute_delay_fixed_mode_returns_base(self, mock_consumer):
		"""Fixed mode ignores cycle_count and cap; delay equals base interval."""
		delay = mock_consumer._compute_retry_delay_ms(
			cycle_count=4, base_min=10, backoff="fixed", cap_min=60
		)
		assert delay == 10 * 60_000

	def test_compute_delay_exponential_doubles_each_cycle(self, mock_consumer):
		"""Each cycle should double until clamped at cap. Jitter is patched
		to 1.0 so the assertion is exact -- the jitter band itself is
		exercised separately in ``test_compute_delay_jitter_within_band``."""
		# With base=1m and cap=60m: 1, 2, 4, 8, 16, 32, 60 (capped), 60, ...
		expected_minutes = [1, 2, 4, 8, 16, 32, 60, 60]
		with patch('mrsal.superclass.random.uniform', return_value=1.0):
			for cycle, base_min in enumerate(expected_minutes):
				delay_ms = mock_consumer._compute_retry_delay_ms(
					cycle_count=cycle, base_min=1, backoff="exponential", cap_min=60
				)
				assert delay_ms == base_min * 60_000, (
					f"cycle {cycle}: expected {base_min}m, got {delay_ms / 60_000:.2f}m"
				)

	def test_compute_delay_jitter_within_band(self, mock_consumer):
		"""Jitter must stay within ±20% of the deterministic value. Sampled
		across many calls because the value is random."""
		samples = [
			mock_consumer._compute_retry_delay_ms(
				cycle_count=0, base_min=10, backoff="exponential", cap_min=60
			)
			for _ in range(200)
		]
		# base=10m, cycle=0 → 10m ±20% = [8m, 12m]
		assert all(0.8 * 10 * 60_000 <= s <= 1.2 * 10 * 60_000 for s in samples)

	def test_compute_delay_clamps_at_cap_post_jitter(self, mock_consumer):
		"""Post-jitter value must never exceed the cap in ms. Without the
		post-jitter clamp, a saturated cycle (raw_min == cap_min) could
		produce delay = cap * 1.2, which the broker's queue TTL would
		silently shorten -- creating a logged/observed mismatch."""
		# Force jitter to its maximum so we hit the over-cap path.
		with patch('mrsal.superclass.random.uniform', return_value=1.2):
			delay_ms = mock_consumer._compute_retry_delay_ms(
				cycle_count=10,  # saturated: 10 * 2**10 >> cap
				base_min=10, backoff="exponential", cap_min=30,
			)
		assert delay_ms == 30 * 60_000

	def test_exponential_mode_stamps_per_message_expiration(self, mock_consumer):
		"""When cycling in exponential mode, the publish properties must carry
		an integer-ms ``expiration_ms`` within the jittered range. The library
		layer (pika str, aio-pika timedelta) converts it for the wire."""
		mock_properties = MagicMock()
		mock_properties.headers = {
			'x-cycle-count': 2,
			'x-first-failure': '2024-01-01T10:00:00Z',
			'x-total-elapsed': 1000,
		}
		mock_properties.content_type = 'application/json'

		_, target_routing, dlx_properties, _, should_cycle, next_delay_ms = mock_consumer._build_dlx_retry_properties(
			properties=mock_properties,
			processing_error="boom",
			original_exchange="orders_exchange",
			original_routing_key="new_order",
			enable_retry_cycles=True,
			retry_cycle_interval=1,
			max_retry_time_limit=120,
			dlx_exchange_name=None,
			retry_backoff="exponential",
			retry_backoff_max=60,
		)

		assert target_routing == "new_order.retry"
		assert should_cycle is True
		assert 'expiration_ms' in dlx_properties
		assert isinstance(dlx_properties['expiration_ms'], int)
		# cycle_count=2 → base * 4 = 4m, ±20%
		assert 0.8 * 4 * 60_000 <= dlx_properties['expiration_ms'] <= 1.2 * 4 * 60_000
		assert next_delay_ms == dlx_properties['expiration_ms']

	def test_exponential_mode_no_expiration_when_exhausted(self, mock_consumer):
		"""On the terminal ``.dlx`` binding (retry budget spent), no per-message
		``expiration_ms`` may be set -- the message must stay parked indefinitely
		for manual review, not expire and get dropped."""
		mock_properties = MagicMock()
		mock_properties.headers = {
			'x-cycle-count': 10,
			'x-first-failure': '2024-01-01T10:00:00Z',
			'x-total-elapsed': 10_000_000,
		}
		mock_properties.content_type = 'application/json'

		_, target_routing, dlx_properties, _, should_cycle, next_delay_ms = mock_consumer._build_dlx_retry_properties(
			properties=mock_properties,
			processing_error="boom",
			original_exchange="orders_exchange",
			original_routing_key="new_order",
			enable_retry_cycles=True,
			retry_cycle_interval=1,
			max_retry_time_limit=1,
			dlx_exchange_name=None,
			retry_backoff="exponential",
			retry_backoff_max=60,
		)

		assert target_routing == "new_order"
		assert should_cycle is False
		assert next_delay_ms is None
		assert 'expiration_ms' not in dlx_properties

	def test_retry_queue_uses_cap_as_ttl_in_exponential_mode(self, mock_consumer):
		"""In exponential mode the queue's ``x-message-ttl`` must be the cap
		so the per-message expiration (always shorter) wins."""
		declared_queues: list[dict] = []
		mock_consumer._declare_exchange = MagicMock()
		mock_consumer._declare_queue = MagicMock(
			side_effect=lambda **kwargs: declared_queues.append(kwargs)
		)
		mock_consumer._declare_queue_binding = MagicMock()

		mock_consumer._setup_exchange_and_queue(
			exchange_name="orders_exchange",
			queue_name="orders_queue",
			exchange_type="direct",
			routing_key="new_order",
			dlx_enable=True,
			enable_retry_cycles=True,
			retry_cycle_interval=5,
			use_quorum_queues=False,
			retry_backoff="exponential",
			retry_backoff_max=45,
		)

		retry_decls = [q for q in declared_queues if q['queue'] == 'orders_queue.retry']
		assert len(retry_decls) == 1
		# TTL must be the cap (45m), NOT the base interval (5m), so per-message
		# expiration always wins.
		assert retry_decls[0]['arguments']['x-message-ttl'] == 45 * 60 * 1000


class TestRetryCyclePreconditions:
	"""start_consumer must reject configurations that would silently drop cycled messages."""

	@pytest.fixture
	def mock_consumer(self):
		consumer = MrsalBlockingAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			dlx_enable=True,
			blocked_connection_timeout=60,
		)
		consumer._connection = MagicMock()
		consumer._channel = MagicMock()
		consumer._consumer_channel = consumer._channel
		consumer._connection.channel.return_value = consumer._channel
		consumer._setup_exchange_and_queue = MagicMock()
		consumer.auto_declare_ok = True
		return consumer

	@pytest.fixture
	def mock_async_consumer(self):
		consumer = MrsalAsyncAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			dlx_enable=True,
		)
		consumer._connection = AsyncMock()
		consumer._connection.is_closed = False
		consumer._channel = AsyncMock()
		consumer._channel.is_closed = False
		consumer._async_setup_exchange_and_queue = AsyncMock()
		return consumer

	def test_rejects_zero_retry_cycle_interval(self, mock_consumer):
		with pytest.raises(MrsalAbortedSetup, match="retry_cycle_interval must be > 0"):
			mock_consumer.start_consumer(
				queue_name="q",
				exchange_name="x",
				exchange_type="direct",
				routing_key="rk",
				callback=Mock(),
				enable_retry_cycles=True,
				retry_cycle_interval=0,
			)

	def test_rejects_negative_retry_cycle_interval(self, mock_consumer):
		with pytest.raises(MrsalAbortedSetup, match="retry_cycle_interval must be > 0"):
			mock_consumer.start_consumer(
				queue_name="q",
				exchange_name="x",
				exchange_type="direct",
				routing_key="rk",
				callback=Mock(),
				enable_retry_cycles=True,
				retry_cycle_interval=-5,
			)

	def test_rejects_none_exchange_type_with_auto_declare(self, mock_consumer):
		"""auto_declare=True + retry_cycles=True + exchange_type=None is incoherent:
		the library is about to declare .retry/.dlx bindings tied to exchange_type,
		and a missing type can't be validated or declared. Fail loud."""
		with pytest.raises(MrsalAbortedSetup, match="requires an explicit exchange_type"):
			mock_consumer.start_consumer(
				queue_name="q",
				exchange_name="x",
				exchange_type=None,
				routing_key="rk",
				callback=Mock(),
				auto_declare=True,
				enable_retry_cycles=True,
			)

	def test_allows_none_exchange_type_when_auto_declare_off(self, mock_consumer):
		"""auto_declare=False + exchange_type=None is allowed: the operator has
		set up the topology themselves; mandatory=True on publish is the safety
		net for routing errors."""
		mock_consumer._channel.consume.return_value = []
		# Should not raise.
		mock_consumer.start_consumer(
			queue_name="q",
			callback=Mock(),
			auto_declare=False,
			enable_retry_cycles=True,
		)

	def test_rejects_fanout_exchange_with_retry_cycles(self, mock_consumer):
		with pytest.raises(MrsalAbortedSetup, match="exchange_type='fanout'"):
			mock_consumer.start_consumer(
				queue_name="q",
				exchange_name="x",
				exchange_type="fanout",
				routing_key="",
				callback=Mock(),
				enable_retry_cycles=True,
			)

	def test_rejects_headers_exchange_with_retry_cycles(self, mock_consumer):
		with pytest.raises(MrsalAbortedSetup, match="exchange_type='headers'"):
			mock_consumer.start_consumer(
				queue_name="q",
				exchange_name="x",
				exchange_type="headers",
				routing_key="rk",
				callback=Mock(),
				enable_retry_cycles=True,
			)

	def test_rejects_topic_wildcard_star(self, mock_consumer):
		with pytest.raises(MrsalAbortedSetup, match="topic wildcard"):
			mock_consumer.start_consumer(
				queue_name="q",
				exchange_name="x",
				exchange_type="topic",
				routing_key="orders.*",
				callback=Mock(),
				enable_retry_cycles=True,
			)

	def test_rejects_topic_wildcard_hash(self, mock_consumer):
		with pytest.raises(MrsalAbortedSetup, match="topic wildcard"):
			mock_consumer.start_consumer(
				queue_name="q",
				exchange_name="x",
				exchange_type="topic",
				routing_key="orders.#",
				callback=Mock(),
				enable_retry_cycles=True,
			)

	def test_concrete_topic_key_is_accepted(self, mock_consumer):
		"""A topic exchange with a concrete (non-wildcard) routing key is fine."""
		mock_consumer._channel.consume.return_value = []
		# Should not raise.
		mock_consumer.start_consumer(
			queue_name="q",
			exchange_name="x",
			exchange_type="topic",
			routing_key="orders.created",
			callback=Mock(),
			enable_retry_cycles=True,
		)

	def test_rejections_skipped_when_retry_cycles_disabled(self, mock_consumer):
		"""Fanout + retry cycles disabled is a valid combination (terminal DLX only)."""
		mock_consumer._channel.consume.return_value = []
		# Should not raise even though the exchange is fanout.
		mock_consumer.start_consumer(
			queue_name="q",
			exchange_name="x",
			exchange_type="fanout",
			routing_key="",
			callback=Mock(),
			enable_retry_cycles=False,
		)

	@pytest.mark.asyncio
	async def test_async_rejects_fanout_exchange(self, mock_async_consumer):
		with pytest.raises(MrsalAbortedSetup, match="exchange_type='fanout'"):
			await mock_async_consumer.start_consumer(
				queue_name="q",
				exchange_name="x",
				exchange_type="fanout",
				routing_key="",
				callback=AsyncMock(),
				enable_retry_cycles=True,
			)

	@pytest.mark.asyncio
	async def test_async_rejects_topic_wildcard(self, mock_async_consumer):
		with pytest.raises(MrsalAbortedSetup, match="topic wildcard"):
			await mock_async_consumer.start_consumer(
				queue_name="q",
				exchange_name="x",
				exchange_type="topic",
				routing_key="orders.*",
				callback=AsyncMock(),
				enable_retry_cycles=True,
			)


class TestRetryQueueSetupFailureEscalation:
	"""Setup failures on the .dlx / .retry queues must abort start_consumer.

	A missing or inconsistent queue makes future DLX publishes unroutable; with
	``mandatory=True`` they'd raise per message, but operators want the failure
	at startup, not later in production."""

	@pytest.fixture
	def consumer(self):
		from mrsal.exceptions import MrsalSetupError
		consumer = MrsalBlockingAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			dlx_enable=True,
			blocked_connection_timeout=60,
		)
		consumer._connection = MagicMock()
		consumer._channel = MagicMock()
		consumer._declare_exchange = MagicMock()
		consumer._declare_queue_binding = MagicMock()
		return consumer

	def test_dlx_queue_setup_failure_raises_aborted_setup(self, consumer):
		from mrsal.exceptions import MrsalSetupError
		# The first _declare_queue call is for .dlx -- make it fail.
		consumer._declare_queue = MagicMock(
			side_effect=MrsalSetupError("inequivalent arg 'x-max-length'")
		)
		with pytest.raises(MrsalAbortedSetup, match="DLX queue .* setup failed"):
			consumer._setup_exchange_and_queue(
				exchange_name="x",
				queue_name="q",
				exchange_type="direct",
				routing_key="rk",
				dlx_enable=True,
				enable_retry_cycles=False,
			)

	def test_retry_queue_setup_failure_raises_aborted_setup(self, consumer):
		from mrsal.exceptions import MrsalSetupError
		# Let .dlx declare succeed, but fail on the second declare (.retry).
		call_log: list[str] = []

		def declare(**kwargs):
			call_log.append(kwargs['queue'])
			if kwargs['queue'].endswith('.retry'):
				raise MrsalSetupError("inequivalent arg 'x-message-ttl'")

		consumer._declare_queue = MagicMock(side_effect=declare)

		with pytest.raises(MrsalAbortedSetup, match="Retry queue .* setup failed"):
			consumer._setup_exchange_and_queue(
				exchange_name="x",
				queue_name="q",
				exchange_type="direct",
				routing_key="rk",
				dlx_enable=True,
				enable_retry_cycles=True,
				retry_cycle_interval=5,
			)
		# Sanity check: .dlx was declared, then .retry tried.
		assert call_log == ['q.dlx', 'q.retry']


class TestSyncDLXPublishMandatoryFlag:
	"""_publish_to_dlx must publish with mandatory=True so unroutable messages
	(e.g. .retry queue missing after a botched redeploy) raise instead of
	being ack-and-discarded by the broker under publisher confirms."""

	def test_sync_publish_uses_mandatory_true(self):
		consumer = MrsalBlockingAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			dlx_enable=True,
			blocked_connection_timeout=60,
		)
		consumer._connection = MagicMock()
		consumer._channel = MagicMock()
		consumer._consumer_channel = MagicMock()

		consumer._publish_to_dlx(
			dlx_exchange="x.dlx",
			routing_key="rk",
			body=b"{}",
			properties={'headers': None},
		)

		dlx_channel = consumer._connection.channel.return_value
		dlx_channel.basic_publish.assert_called_once()
		_, kwargs = dlx_channel.basic_publish.call_args
		assert kwargs['mandatory'] is True, (
			"Without mandatory=True, unroutable DLX publishes are silently dropped "
			"by the broker under publisher confirms -- defeats the whole point of #84."
		)

	@pytest.mark.asyncio
	async def test_async_publish_uses_mandatory_true(self):
		"""Pin aio-pika's mandatory=True default so an upstream default flip
		doesn't quietly re-open the silent-drop path on the async side."""
		consumer = MrsalAsyncAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			dlx_enable=True,
		)
		consumer._connection = AsyncMock()
		consumer._connection.is_closed = False
		dlx_channel = AsyncMock()
		dlx_channel.is_closed = False
		dlx_exchange = AsyncMock()
		dlx_channel.get_exchange = AsyncMock(return_value=dlx_exchange)
		consumer._connection.channel = AsyncMock(return_value=dlx_channel)
		consumer._dlx_publish_channel = None

		await consumer._publish_to_dlx(
			dlx_exchange="x.dlx",
			routing_key="rk",
			body=b"{}",
			properties={'headers': None},
		)

		dlx_exchange.publish.assert_awaited_once()
		_, kwargs = dlx_exchange.publish.call_args
		assert kwargs.get('mandatory') is True, (
			"Pin mandatory=True explicitly on the async publish -- aio-pika's default "
			"may flip in a future release, and silent-drop is exactly the bug #84 fixed."
		)


class TestAsyncRetryQueueSetup:
	"""Mirror the sync .retry-queue declaration coverage for the async setup path."""

	@pytest.mark.asyncio
	async def test_async_retry_queue_declared_with_ttl_and_dead_letter_args(self):
		consumer = MrsalAsyncAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			dlx_enable=True,
			use_quorum_queues=False,
		)
		consumer._connection = AsyncMock()
		consumer._channel = AsyncMock()
		# get_exchange is awaited during binding setup.
		consumer._channel.get_exchange = AsyncMock(return_value=AsyncMock())

		declared: list[dict] = []

		async def fake_declare_queue(**kwargs):
			declared.append(kwargs)
			return AsyncMock()

		async def fake_declare_exchange(**kwargs):
			return AsyncMock()

		async def fake_declare_binding(**kwargs):
			return None

		consumer._async_declare_queue = fake_declare_queue
		consumer._async_declare_exchange = fake_declare_exchange
		consumer._async_declare_queue_binding = fake_declare_binding

		await consumer._async_setup_exchange_and_queue(
			exchange_name="orders_exchange",
			queue_name="orders_queue",
			exchange_type="direct",
			routing_key="new_order",
			dlx_enable=True,
			enable_retry_cycles=True,
			retry_cycle_interval=5,
			use_quorum_queues=False,
			retry_backoff="fixed",
		)

		retry_decls = [q for q in declared if q['queue_name'] == 'orders_queue.retry']
		assert len(retry_decls) == 1, f"expected one .retry queue declaration, got {declared}"
		retry_args = retry_decls[0]['arguments']
		assert retry_args['x-message-ttl'] == 5 * 60 * 1000
		assert retry_args['x-dead-letter-exchange'] == 'orders_exchange'
		assert retry_args['x-dead-letter-routing-key'] == 'new_order'

	@pytest.mark.asyncio
	async def test_async_retry_queue_setup_failure_raises_aborted_setup(self):
		from mrsal.exceptions import MrsalSetupError
		consumer = MrsalAsyncAMQP(
			host="localhost",
			port=5672,
			credentials=("user", "password"),
			virtual_host="testboi",
			dlx_enable=True,
		)
		consumer._connection = AsyncMock()
		consumer._channel = AsyncMock()
		consumer._channel.get_exchange = AsyncMock(return_value=AsyncMock())

		async def declare(**kwargs):
			if kwargs['queue_name'].endswith('.retry'):
				raise MrsalSetupError("inequivalent arg 'x-message-ttl'")
			return AsyncMock()

		consumer._async_declare_queue = declare
		consumer._async_declare_exchange = AsyncMock(return_value=AsyncMock())
		consumer._async_declare_queue_binding = AsyncMock()

		with pytest.raises(MrsalAbortedSetup, match="Retry queue .* setup failed"):
			await consumer._async_setup_exchange_and_queue(
				exchange_name="x",
				queue_name="q",
				exchange_type="direct",
				routing_key="rk",
				dlx_enable=True,
				enable_retry_cycles=True,
				retry_cycle_interval=5,
			)
