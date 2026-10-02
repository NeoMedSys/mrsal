import asyncio
import logging
import time
import aiormq
import pytest
from aio_pika.exceptions import AuthenticationError, ChannelInvalidStateError, DeliveryError
from aio_pika.tools import CallbackCollection
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from mrsal import config
from mrsal.amqp import async_amqp, subclass
from mrsal.amqp.subclass import MrsalAsyncAMQP
from mrsal.config import AioPikaAttributes
from mrsal.exceptions import MrsalAbortedSetup, MrsalDLXPublishTimeout, MrsalSetupError
from mrsal.metrics import MetricsHooks
from pydantic import ValidationError
from tenacity import wait_fixed, wait_none

from tests.conftest import ExpectedPayload, make_queue_with_messages


# Configuration override (this test file uses ssl explicitly).
SETUP_ARGS = {
	'host': 'localhost',
	'port': 5672,
	'credentials': ('user', 'password'),
	'virtual_host': 'testboi',
	'ssl': False,
	'heartbeat': 60,
	'prefetch_count': 1
}


# Fixture to mock the async connection and its methods - SYNC fixture
@pytest.fixture
def mock_amqp_connection():
	with patch('aio_pika.connect_robust', new_callable=AsyncMock) as mock_connect_robust:
		mock_channel = AsyncMock()
		mock_channel.close_callbacks = MagicMock()
		mock_channel.is_closed = False
		mock_connection = AsyncMock()
		mock_connection.close_callbacks = MagicMock()
		mock_connection.is_closed = False
		mock_connection.channel.return_value = mock_channel

		mock_connect_robust.return_value = mock_connection

		# Return the connection and channel
		return mock_connection, mock_channel

@pytest.fixture
def amqp_consumer(mock_amqp_connection):
	# No await needed - it's a sync fixture now
	mock_connection, mock_channel = mock_amqp_connection
	consumer = MrsalAsyncAMQP(**SETUP_ARGS)
	consumer._connection = mock_connection  # Inject the mocked connection
	consumer._channel = mock_channel
	return consumer  # Return the consumer instance


@pytest.mark.asyncio
async def test_valid_message_processing(amqp_consumer):
	"""start_consumer must invoke the callback for a healthy message."""
	consumer = amqp_consumer

	valid_body = b'{"id": 1, "name": "Test", "active": true}'
	mock_message = AsyncMock(body=valid_body, ack=AsyncMock(), reject=AsyncMock())
	mock_message.configure_mock(app_id="test_app", message_id="12345", headers=None, redelivered=False)

	mock_queue, _ = make_queue_with_messages([mock_message])
	consumer._channel.declare_queue.return_value = mock_queue

	mock_callback = AsyncMock()

	await consumer.start_consumer(
		queue_name='test_q',
		callback=mock_callback,
		routing_key='test_route',
		exchange_name='test_x',
		exchange_type='direct',
		auto_ack=True,
		dlx_enable=False,
	)

	mock_callback.assert_awaited_once()


@pytest.mark.asyncio
async def test_callback_receives_validated_instance(amqp_consumer):
	"""Regression for 3.6.0: when payload_model is set, the callback's body
	argument is the validated model instance, not raw bytes.
	"""
	consumer = amqp_consumer
	valid_body = b'{"id": 1, "name": "Test", "active": true}'

	mock_message = AsyncMock(body=valid_body, ack=AsyncMock(), reject=AsyncMock())
	mock_message.configure_mock(app_id="test_app", message_id="12345", headers=None, redelivered=False)

	mock_queue, _ = make_queue_with_messages([mock_message])
	consumer._channel.declare_queue.return_value = mock_queue

	received = {}

	async def callback(message, properties, body):
		received['body'] = body

	await consumer.start_consumer(
		queue_name='test_q',
		callback=callback,
		routing_key='test_route',
		exchange_name='test_x',
		exchange_type='direct',
		payload_model=ExpectedPayload,
		auto_ack=True,
		dlx_enable=False,
	)

	assert isinstance(received['body'], ExpectedPayload)
	assert received['body'].id == 1
	assert received['body'].name == 'Test'
	assert received['body'].active is True


@pytest.mark.asyncio
async def test_invalid_payload_validation(amqp_consumer):
	"""auto_ack=True: validation failure must skip the callback and not raise."""
	invalid_payload = b'{"id": "wrong_type", "name": 123, "active": "maybe"}'
	consumer = amqp_consumer

	mock_message = AsyncMock(body=invalid_payload, ack=AsyncMock(), reject=AsyncMock())
	mock_message.configure_mock(app_id="test_app", message_id="12345", headers=None, redelivered=False, routing_key="rk")

	mock_queue, _ = make_queue_with_messages([mock_message])
	consumer._channel.declare_queue.return_value = mock_queue

	mock_callback = AsyncMock()

	await consumer.start_consumer(
		queue_name='test_q',
		callback=mock_callback,
		routing_key='test_route',
		exchange_name='test_x',
		exchange_type='direct',
		payload_model=ExpectedPayload,
		auto_ack=True,
		dlx_enable=False,
	)

	mock_callback.assert_not_called()
	# auto_ack=True opts out of DLX; broker already acked
	mock_message.reject.assert_not_called()
	mock_message.ack.assert_not_called()


@pytest.mark.asyncio
async def test_requeue_on_invalid_message(amqp_consumer):
	"""auto_ack=False + invalid payload routes the message to DLX (reject without requeue)."""
	invalid_payload = b'{"id": "wrong_type", "name": 123, "active": "maybe"}'
	consumer = amqp_consumer

	mock_message = AsyncMock(body=invalid_payload, ack=AsyncMock(), reject=AsyncMock())
	mock_message.configure_mock(app_id="test_app", message_id="12345", headers=None, redelivered=False, routing_key="rk")

	mock_queue, _ = make_queue_with_messages([mock_message])
	consumer._channel.declare_queue.return_value = mock_queue

	mock_callback = AsyncMock()

	# Patch out the DLX retry-cycle publish (we're not testing that path here).
	with patch.object(consumer, '_async_publish_to_dlx_with_retry_cycle', AsyncMock()) as dlx_spy:
		await consumer.start_consumer(
			queue_name='test_q',
			callback=mock_callback,
			routing_key='test_route',
			exchange_name='test_x',
			exchange_type='direct',
			payload_model=ExpectedPayload,
			auto_ack=False,
		)

	mock_callback.assert_not_called()
	# Validation failure should route through DLX retry cycle with dlx_enable=True (default).
	dlx_spy.assert_awaited_once()
	mock_message.ack.assert_not_called()


@pytest.mark.asyncio
async def test_setup_async_connection_reraises_unexpected_exception():
	"""Unexpected exceptions from setup_async_connection must propagate, not be swallowed."""
	consumer = MrsalAsyncAMQP(**SETUP_ARGS)

	with patch('mrsal.amqp.async_amqp.connect_robust',
			new_callable=AsyncMock,
			side_effect=RuntimeError("disk on fire")):
		with pytest.raises(RuntimeError, match="disk on fire"):
			await consumer.setup_async_connection()


@pytest.mark.asyncio
async def test_ensure_consumer_channel_closes_prior_open_channel():
	"""Prevents the channel leak that previously occurred on tenacity retry."""
	consumer = MrsalAsyncAMQP(**SETUP_ARGS)

	stale_channel = AsyncMock()
	stale_channel.is_closed = False
	fresh_channel = AsyncMock()
	consumer._channel = stale_channel
	consumer._connection = AsyncMock()
	consumer._connection.close_callbacks = MagicMock()
	consumer._connection.is_closed = False
	consumer._connection.channel = AsyncMock(return_value=fresh_channel)

	await consumer._ensure_consumer_channel()

	stale_channel.close.assert_awaited_once()
	fresh_channel.set_qos.assert_awaited_once_with(prefetch_count=consumer.prefetch_count)
	assert consumer._channel is fresh_channel


@pytest.mark.asyncio
async def test_ensure_consumer_channel_closes_fresh_channel_if_set_qos_fails():
	"""If set_qos raises, the freshly opened channel must be closed (no leak)."""
	consumer = MrsalAsyncAMQP(**SETUP_ARGS)
	consumer._channel = None

	fresh_channel = AsyncMock()
	fresh_channel.set_qos = AsyncMock(side_effect=RuntimeError("qos boom"))
	consumer._connection = AsyncMock()
	consumer._connection.close_callbacks = MagicMock()
	consumer._connection.is_closed = False
	consumer._connection.channel = AsyncMock(return_value=fresh_channel)

	with pytest.raises(RuntimeError, match="qos boom"):
		await consumer._ensure_consumer_channel()

	fresh_channel.close.assert_awaited_once()
	assert consumer._channel is None


@pytest.mark.asyncio
async def test_ensure_async_connection_reconnects_stale_connection():
	"""Closed-but-non-None connection must be reconnected, not reused."""
	consumer = MrsalAsyncAMQP(**SETUP_ARGS)

	stale_connection = AsyncMock()
	stale_connection.is_closed = True
	consumer._connection = stale_connection

	with patch.object(consumer, 'setup_async_connection', new_callable=AsyncMock) as mock_setup:
		await consumer._ensure_async_connection()

	stale_connection.close.assert_awaited_once()
	mock_setup.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_async_connection_noop_when_connection_is_open():
	"""Healthy connection must not be torn down."""
	consumer = MrsalAsyncAMQP(**SETUP_ARGS)
	open_connection = AsyncMock()
	open_connection.is_closed = False
	consumer._connection = open_connection

	with patch.object(consumer, 'setup_async_connection', new_callable=AsyncMock) as mock_setup:
		await consumer._ensure_async_connection()

	open_connection.close.assert_not_called()
	mock_setup.assert_not_called()


@pytest.mark.asyncio
async def test_auto_ack_true_sets_broker_no_ack_and_drops_failures(amqp_consumer):
	"""auto_ack=True (with dlx_enable=False) must (a) pass no_ack=True to queue.iterator
	and (b) drop failures silently -- the broker has already acked, there is no DLX to fall
	back to."""
	consumer = amqp_consumer

	valid_body = b'{"id": 1, "name": "Test", "active": true}'
	mock_message = AsyncMock(body=valid_body, ack=AsyncMock(), reject=AsyncMock())
	mock_message.configure_mock(app_id="test_app", message_id="12345", routing_key="rk", headers=None, redelivered=False)

	mock_queue, fake_it = make_queue_with_messages([mock_message])
	consumer._channel.declare_queue.return_value = mock_queue

	failing_callback = AsyncMock(side_effect=RuntimeError("callback boom"))
	dlx_spy = AsyncMock()

	with patch.object(consumer, '_async_setup_exchange_and_queue', AsyncMock(return_value=mock_queue)), \
		patch.object(consumer, '_async_publish_to_dlx_with_retry_cycle', dlx_spy):
		consumer.auto_declare_ok = True
		await consumer.start_consumer(
			queue_name='test_q',
			callback=failing_callback,
			routing_key='test_route',
			exchange_name='test_x',
			exchange_type='direct',
			auto_ack=True,
			dlx_enable=False,
		)

	assert fake_it.iterator_call_kwargs == {'no_ack': True}
	dlx_spy.assert_not_called()
	mock_message.ack.assert_not_called()
	mock_message.reject.assert_not_called()


@pytest.mark.asyncio
async def test_auto_ack_true_with_dlx_enable_true_raises_at_setup(amqp_consumer):
	"""auto_ack=True + dlx_enable=True is rejected at setup: once the broker has acked,
	failed messages cannot be routed to the DLX, so the combination is meaningless.

	Asserts the raise happens before any broker IO, so a future regression that moves
	the check below ``_ensure_async_connection`` would fail this test.
	"""
	consumer = amqp_consumer

	with patch.object(consumer, '_ensure_async_connection', AsyncMock()) as ensure_spy, \
		pytest.raises(MrsalAbortedSetup, match="auto_ack=True is incompatible with dlx_enable=True"):
		await consumer.start_consumer(
			queue_name='test_q',
			callback=AsyncMock(),
			routing_key='test_route',
			exchange_name='test_x',
			exchange_type='direct',
			auto_ack=True,
			dlx_enable=True,
		)

	ensure_spy.assert_not_called()


def test_aio_pika_attributes_from_message_populates_all_fields():
	"""AioPikaAttributes.from_message must mirror the full pika.BasicProperties surface."""
	ts = datetime(2026, 1, 1, 12, 0, 0)
	fake_message = Mock(
		message_id="m1",
		app_id="a1",
		headers={"x": "y"},
		correlation_id="c1",
		reply_to="r1",
		content_type="application/json",
		content_encoding="utf-8",
		delivery_mode=2,
		expiration="60000",
		priority=5,
		timestamp=ts,
		type="event",
		user_id="u1",
	)

	props = AioPikaAttributes.from_message(fake_message)

	assert props.message_id == "m1"
	assert props.app_id == "a1"
	assert props.headers == {"x": "y"}
	assert props.correlation_id == "c1"
	assert props.reply_to == "r1"
	assert props.content_type == "application/json"
	assert props.content_encoding == "utf-8"
	assert props.delivery_mode == 2
	assert props.expiration == "60000"
	assert props.priority == 5
	assert props.timestamp == ts
	assert props.type == "event"
	assert props.user_id == "u1"


def _make_messages(n):
	"""Build ``n`` AsyncMock messages with the attributes the consumer reads."""
	out = []
	for i in range(n):
		m = AsyncMock(body=b'{}', ack=AsyncMock(), reject=AsyncMock())
		m.configure_mock(
			app_id=f"app{i}", message_id=f"id{i}",
			headers=None, redelivered=False, routing_key="rk",
		)
		out.append(m)
	return out


@pytest.mark.asyncio
async def test_iterator_used_as_async_context_manager(amqp_consumer):
	"""start_consumer must enter and exit queue.iterator() as an async context manager.

	Closes a regression noted in the #74 review comment: without ``async with``,
	consumer cancellation isn't deterministically delivered to the broker on
	exception or GC.
	"""
	consumer = amqp_consumer

	aenter_calls = []
	aexit_calls = []

	class TrackedIterator:
		def __init__(self, messages):
			self._messages = list(messages)
			self.iterator_call_kwargs = None

		async def __aenter__(self):
			aenter_calls.append(True)
			return self

		async def __aexit__(self, exc_type, exc, tb):
			aexit_calls.append(True)
			return False

		def __aiter__(self):
			return self

		async def __anext__(self):
			if not self._messages:
				raise StopAsyncIteration
			return self._messages.pop(0)

		async def close(self):
			self._messages = []

	msg = _make_messages(1)[0]
	tracked = TrackedIterator([msg])
	mock_queue = AsyncMock()
	mock_queue.iterator = MagicMock(return_value=tracked)
	consumer._channel.declare_queue.return_value = mock_queue

	await consumer.start_consumer(
		queue_name='test_q',
		callback=AsyncMock(),
		routing_key='rk',
		exchange_name='test_x',
		exchange_type='direct',
		auto_ack=True,
		dlx_enable=False,
	)

	assert aenter_calls == [True]
	assert aexit_calls == [True]


@pytest.mark.asyncio
async def test_max_concurrent_tasks_runs_callbacks_in_parallel(amqp_consumer):
	"""Acceptance: with max_concurrent_tasks=N, up to N callbacks run concurrently.

	Verified via a max-observed-concurrency counter rather than wall-clock timing
	so the test stays robust under CI load.
	"""
	consumer = amqp_consumer

	N = 4
	messages = _make_messages(N)
	mock_queue, _ = make_queue_with_messages(messages)
	consumer._channel.declare_queue.return_value = mock_queue

	active = 0
	max_observed = 0
	all_started = asyncio.Event()

	async def slow_callback(message, properties, body):
		nonlocal active, max_observed
		active += 1
		max_observed = max(max_observed, active)
		if active >= N:
			all_started.set()
		# Hold the slot until every callback has entered, proving they overlap.
		try:
			await asyncio.wait_for(all_started.wait(), timeout=1.0)
		finally:
			active -= 1

	await consumer.start_consumer(
		queue_name='test_q',
		callback=slow_callback,
		routing_key='rk',
		exchange_name='test_x',
		exchange_type='direct',
		auto_ack=True,
		dlx_enable=False,
		max_concurrent_tasks=N,
	)

	assert max_observed == N, f"Expected {N} concurrent callbacks, observed {max_observed}"


@pytest.mark.asyncio
async def test_sequential_mode_does_not_overlap_callbacks(amqp_consumer):
	"""Acceptance: with max_concurrent_tasks=None, callbacks run one at a time."""
	consumer = amqp_consumer

	active = 0
	max_observed = 0

	async def callback(message, properties, body):
		nonlocal active, max_observed
		active += 1
		max_observed = max(max_observed, active)
		await asyncio.sleep(0.02)
		active -= 1

	messages = _make_messages(3)
	mock_queue, _ = make_queue_with_messages(messages)
	consumer._channel.declare_queue.return_value = mock_queue

	await consumer.start_consumer(
		queue_name='test_q',
		callback=callback,
		routing_key='rk',
		exchange_name='test_x',
		exchange_type='direct',
		auto_ack=True,
		dlx_enable=False,
	)

	assert max_observed == 1


@pytest.mark.asyncio
async def test_stop_exits_loop_after_inflight_messages_finish(amqp_consumer):
	"""Acceptance: await stop() exits the loop cleanly after in-flight messages finish."""
	consumer = amqp_consumer

	proceed = asyncio.Event()
	started_event = asyncio.Event()
	in_flight = 0
	completed = []

	async def slow_callback(message, properties, body):
		nonlocal in_flight
		in_flight += 1
		if in_flight >= 2:
			started_event.set()
		await proceed.wait()
		completed.append(message.message_id)

	messages = _make_messages(5)
	mock_queue, _ = make_queue_with_messages(messages)
	consumer._channel.declare_queue.return_value = mock_queue

	consumer_task = asyncio.create_task(consumer.start_consumer(
		queue_name='test_q',
		callback=slow_callback,
		routing_key='rk',
		exchange_name='test_x',
		exchange_type='direct',
		auto_ack=True,
		dlx_enable=False,
		max_concurrent_tasks=2,
	))

	await asyncio.wait_for(started_event.wait(), timeout=1.0)

	# Request graceful stop while two callbacks are mid-flight.
	await consumer.stop()

	# start_consumer must NOT have returned yet -- it's draining in-flight tasks.
	await asyncio.sleep(0)
	assert not consumer_task.done(), "start_consumer returned before draining in-flight tasks"

	# Release the in-flight callbacks; consumer should drain and return.
	proceed.set()
	await asyncio.wait_for(consumer_task, timeout=2.0)

	# The two in-flight messages completed; the remaining three were not processed.
	assert len(completed) == 2


@pytest.mark.asyncio
async def test_no_unacked_messages_dangling_on_graceful_stop(amqp_consumer):
	"""Acceptance: every in-flight message is acked before start_consumer returns from stop()."""
	consumer = amqp_consumer

	proceed = asyncio.Event()
	started_event = asyncio.Event()
	in_flight = 0

	async def slow_callback(message, properties, body):
		nonlocal in_flight
		in_flight += 1
		if in_flight >= 2:
			started_event.set()
		await proceed.wait()

	messages = _make_messages(5)
	mock_queue, _ = make_queue_with_messages(messages)
	consumer._channel.declare_queue.return_value = mock_queue

	consumer_task = asyncio.create_task(consumer.start_consumer(
		queue_name='test_q',
		callback=slow_callback,
		routing_key='rk',
		exchange_name='test_x',
		exchange_type='direct',
		auto_ack=False,
		max_concurrent_tasks=2,
	))

	await asyncio.wait_for(started_event.wait(), timeout=1.0)
	await consumer.stop()
	proceed.set()
	await asyncio.wait_for(consumer_task, timeout=2.0)

	# The two in-flight messages must be acked.
	messages[0].ack.assert_awaited_once()
	messages[1].ack.assert_awaited_once()
	# The remaining three were never dispatched; nothing touched them.
	for m in messages[2:]:
		m.ack.assert_not_called()
		m.reject.assert_not_called()


@pytest.mark.asyncio
async def test_stop_is_idempotent_before_start_consumer():
	"""stop() must be safe to call before start_consumer has ever run."""
	consumer = MrsalAsyncAMQP(**SETUP_ARGS)
	# Should not raise even though _stop_event and _consumer_iterator are None.
	await consumer.stop()


@pytest.mark.asyncio
async def test_stop_event_preserved_across_start_consumer_reentry(amqp_consumer):
	"""M1 regression: a stop() that fires between tenacity retries must not be lost.

	Simulates the state after tenacity caught a connection error and is about to
	re-enter start_consumer: ``_stop_event`` exists and is set. The new attempt
	must observe the set state and exit immediately, not clobber it with a fresh
	unset Event.
	"""
	consumer = amqp_consumer

	# Pre-set the stop event as if stop() was called during exponential backoff.
	consumer._stop_event = asyncio.Event()
	consumer._stop_event.set()
	preexisting_event = consumer._stop_event

	msg = _make_messages(1)[0]
	mock_queue, _ = make_queue_with_messages([msg])
	consumer._channel.declare_queue.return_value = mock_queue

	callback = AsyncMock()
	await consumer.start_consumer(
		queue_name='test_q',
		callback=callback,
		routing_key='rk',
		exchange_name='test_x',
		exchange_type='direct',
		auto_ack=True,
		dlx_enable=False,
	)

	callback.assert_not_called()
	# The event reference must be the same (not replaced) and still set.
	assert consumer._stop_event is preexisting_event
	assert consumer._stop_event.is_set()


@pytest.mark.asyncio
async def test_drain_timeout_cancels_hung_inflight_tasks(amqp_consumer):
	"""M2: drain_timeout must cancel in-flight tasks that never finish."""
	consumer = amqp_consumer

	started_event = asyncio.Event()
	hung = asyncio.Event()  # never set

	async def hung_callback(message, properties, body):
		started_event.set()
		await hung.wait()

	messages = _make_messages(2)
	mock_queue, _ = make_queue_with_messages(messages)
	consumer._channel.declare_queue.return_value = mock_queue

	consumer_task = asyncio.create_task(consumer.start_consumer(
		queue_name='test_q',
		callback=hung_callback,
		routing_key='rk',
		exchange_name='test_x',
		exchange_type='direct',
		auto_ack=True,
		dlx_enable=False,
		max_concurrent_tasks=2,
		drain_timeout=0.1,
	))

	await asyncio.wait_for(started_event.wait(), timeout=1.0)
	await consumer.stop()

	# Without drain_timeout this would hang forever; the timeout must force return.
	await asyncio.wait_for(consumer_task, timeout=2.0)


class _BlockingQueueIterator:
	"""Async-iterator + context-manager whose __anext__ blocks until close() is called.

	Models an idle aio-pika queue with no pending deliveries.
	"""
	def __init__(self):
		self._closed = asyncio.Event()
		# Set once the consumer waits for a delivery on the idle queue.
		self.waiting = asyncio.Event()
		self.iterator_call_kwargs = None
		self.close_calls = 0

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	def __aiter__(self):
		return self

	async def __anext__(self):
		self.waiting.set()
		await self._closed.wait()
		raise StopAsyncIteration

	async def close(self):
		self.close_calls += 1
		self._closed.set()


class _QueueThenBlock(_BlockingQueueIterator):
	"""Yields the given messages, then blocks like an idle queue until close()."""
	def __init__(self, messages):
		super().__init__()
		self._messages = list(messages)

	async def __anext__(self):
		if self._messages:
			return self._messages.pop(0)
		return await super().__anext__()


@pytest.mark.asyncio
async def test_stop_wakes_idle_iterator(amqp_consumer):
	"""m5: stop() must close the iterator so an idle consumer wakes up promptly.

	Without ``_consumer_iterator.close()`` in stop(), an idle ``async for ... in it``
	would block on the broker forever even after stop_event is set.
	"""
	consumer = amqp_consumer

	blocking_it = _BlockingQueueIterator()
	mock_queue = AsyncMock()

	def iterator_factory(**kwargs):
		blocking_it.iterator_call_kwargs = kwargs
		return blocking_it

	mock_queue.iterator = MagicMock(side_effect=iterator_factory)
	consumer._channel.declare_queue.return_value = mock_queue

	callback = AsyncMock()
	consumer_task = asyncio.create_task(consumer.start_consumer(
		queue_name='test_q',
		callback=callback,
		routing_key='rk',
		exchange_name='test_x',
		exchange_type='direct',
		auto_ack=True,
		dlx_enable=False,
	))

	await asyncio.wait_for(blocking_it.waiting.wait(), timeout=1.0)
	assert not consumer_task.done(), "consumer should be blocked on idle iterator"

	await consumer.stop()
	await asyncio.wait_for(consumer_task, timeout=1.0)

	callback.assert_not_called()
	assert blocking_it.close_calls >= 1


# --- Connection loss on the consumer (#105) -----------------------------------

class _RebuildHarness:
	"""Old connection/channel with real aio-pika CallbackCollections, plus a
	rebuilt connection serving an empty queue so a retried consumer returns."""

	def __init__(self, consumer, monkeypatch, old_iterator):
		monkeypatch.setattr(async_amqp, '_CONSUMER_RETRY_WAIT', wait_none())
		self.old_connection, self.old_channel = consumer._connection, consumer._channel
		self.old_iterator = old_iterator
		# The real collection type aio-pika fires, called the way aio-pika calls it.
		self.old_connection.close_callbacks = CallbackCollection(self.old_connection)
		self.old_channel.close_callbacks = CallbackCollection(self.old_channel)
		old_queue = AsyncMock()
		old_queue.iterator = MagicMock(return_value=old_iterator)
		self.old_channel.declare_queue.return_value = old_queue

		self.new_queue, _ = make_queue_with_messages([])
		new_channel = AsyncMock()
		new_channel.is_closed = False
		new_channel.close_callbacks = MagicMock()
		new_channel.declare_queue.return_value = self.new_queue
		self.new_connection = AsyncMock()
		self.new_connection.is_closed = False
		self.new_connection.close_callbacks = MagicMock()
		self.new_connection.channel.return_value = new_channel

		async def _reconnect():
			consumer._connection = self.new_connection
		consumer.setup_async_connection = AsyncMock(side_effect=_reconnect)
		self.consumer = consumer

	def start(self, **overrides):
		kwargs = dict(
			queue_name='test_q', callback=AsyncMock(), routing_key='rk',
			exchange_name='test_x', exchange_type='direct', auto_ack=True, dlx_enable=False,
		)
		kwargs.update(overrides)
		return asyncio.create_task(self.consumer.start_consumer(**kwargs))

	async def wait_consuming(self):
		"""Until the consumer waits for a delivery on the idle old queue."""
		await asyncio.wait_for(self.old_iterator.waiting.wait(), timeout=1.0)

	def assert_rebuilt(self):
		self.consumer.setup_async_connection.assert_awaited_once()
		self.new_queue.iterator.assert_called_once()
		# Callbacks were removed from the old handles before they were dropped.
		assert len(self.old_connection.close_callbacks) == 0
		assert len(self.old_channel.close_callbacks) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('lost_handle', ['connection', 'channel'])
async def test_connection_loss_on_idle_consumer_rebuilds_and_resumes(amqp_consumer, monkeypatch, lost_handle):
	"""#105: aio-pika's robust reconnect can fail to restore the channel, leaving an
	idle iterator waiting forever. A close of the connection or consumer channel must
	end the loop, drop the old handles, and let the start_consumer retry rebuild."""
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_BlockingQueueIterator())
	consumer_task = harness.start()
	await harness.wait_consuming()
	assert not consumer_task.done(), "consumer should be blocked on idle iterator"

	lost = harness.old_connection if lost_handle == 'connection' else harness.old_channel
	await lost.close_callbacks(ConnectionError('Broken pipe'))
	await asyncio.wait_for(consumer_task, timeout=1.0)

	harness.old_connection.close.assert_awaited()
	harness.assert_rebuilt()


@pytest.mark.asyncio
async def test_connection_lost_while_callback_runs_rebuilds_after_it(amqp_consumer, monkeypatch):
	"""Loss during a running callback: the callback finishes, then the loop rebuilds."""
	release = asyncio.Event()
	running = asyncio.Event()

	async def slow_callback(message, properties, body):
		running.set()
		await release.wait()

	message = AsyncMock(body=b'{}', ack=AsyncMock(), reject=AsyncMock())
	message.configure_mock(app_id="a", message_id="m1", headers=None, redelivered=False, routing_key="rk", delivery_tag=1)
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_QueueThenBlock([message]))
	consumer_task = harness.start(callback=slow_callback, auto_ack=False)
	await asyncio.wait_for(running.wait(), timeout=1.0)

	await harness.old_connection.close_callbacks(ConnectionError('Broken pipe'))
	release.set()
	await asyncio.wait_for(consumer_task, timeout=1.0)

	harness.assert_rebuilt()


@pytest.mark.asyncio
async def test_ack_raising_channel_invalid_state_rebuilds(amqp_consumer, monkeypatch):
	"""The ack on a dead channel raises ChannelInvalidStateError; that must reach the
	retry and rebuild, not end start_consumer."""
	message = AsyncMock(body=b'{}', reject=AsyncMock())
	message.ack = AsyncMock(side_effect=ChannelInvalidStateError('No active transport in channel'))
	message.configure_mock(app_id="a", message_id="m1", headers=None, redelivered=False, routing_key="rk", delivery_tag=1)
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_QueueThenBlock([message]))

	await asyncio.wait_for(harness.start(auto_ack=False), timeout=1.0)

	harness.assert_rebuilt()


@pytest.mark.asyncio
async def test_connection_error_in_concurrent_task_rebuilds(amqp_consumer, monkeypatch):
	"""max_concurrent_tasks path: a task's connection error is not swallowed; it
	ends the loop as a connection loss and the consumer is rebuilt."""
	message = AsyncMock(body=b'{}', reject=AsyncMock())
	message.ack = AsyncMock(side_effect=ChannelInvalidStateError('No active transport in channel'))
	message.configure_mock(app_id="a", message_id="m1", headers=None, redelivered=False, routing_key="rk", delivery_tag=1)
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_QueueThenBlock([message]))

	await asyncio.wait_for(harness.start(auto_ack=False, max_concurrent_tasks=2), timeout=1.0)

	harness.assert_rebuilt()


@pytest.mark.asyncio
async def test_stop_racing_a_connection_loss_ends_the_consumer(amqp_consumer, monkeypatch):
	"""A stop seen together with a loss wins: start_consumer returns, no rebuild."""
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_BlockingQueueIterator())
	consumer_task = harness.start()
	await harness.wait_consuming()

	harness.consumer._stop_event.set()
	await harness.old_connection.close_callbacks(ConnectionError('Broken pipe'))
	await asyncio.wait_for(consumer_task, timeout=1.0)

	harness.consumer.setup_async_connection.assert_not_awaited()


@pytest.mark.asyncio
async def test_close_ends_a_running_consumer_without_reconnecting(amqp_consumer, monkeypatch):
	"""close() / __aexit__ is a deliberate shutdown, not a connection loss."""
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_BlockingQueueIterator())
	old_connection = harness.old_connection

	async def _close_fires_callbacks():
		await old_connection.close_callbacks(None)
	old_connection.close = AsyncMock(side_effect=_close_fires_callbacks)

	consumer_task = harness.start()
	await harness.wait_consuming()
	await harness.consumer.close()
	await asyncio.wait_for(consumer_task, timeout=1.0)

	harness.consumer.setup_async_connection.assert_not_awaited()


@pytest.mark.asyncio
async def test_connection_error_during_redeclare_rebuilds_the_connection(amqp_consumer, monkeypatch):
	"""The rebuild re-declares topology; a connection error there must be retried,
	not turned into MrsalAbortedSetup by the async declare helpers, and the retry
	must connect fresh rather than reuse the half-dead connection."""
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_BlockingQueueIterator())
	harness.old_channel.declare_queue.side_effect = ConnectionError('Broken pipe')

	await asyncio.wait_for(harness.start(), timeout=1.0)

	harness.old_connection.close.assert_awaited()
	harness.assert_rebuilt()


@pytest.mark.asyncio
async def test_stop_during_backoff_against_unreachable_broker_returns(monkeypatch):
	"""The broker is down from the start, so the loop never runs; stop() during
	the retry backoff must end start_consumer at once, not after the 10s sleep."""
	monkeypatch.setattr(async_amqp, '_CONSUMER_RETRY_WAIT', wait_fixed(10))
	consumer = MrsalAsyncAMQP(**SETUP_ARGS)
	attempted = asyncio.Event()

	async def _refused():
		attempted.set()
		raise ConnectionError('Connection refused')
	consumer.setup_async_connection = AsyncMock(side_effect=_refused)

	consumer_task = asyncio.create_task(consumer.start_consumer(
		queue_name='test_q', callback=AsyncMock(), routing_key='rk',
		exchange_name='test_x', exchange_type='direct', auto_ack=True, dlx_enable=False,
	))
	await asyncio.wait_for(attempted.wait(), timeout=1.0)
	await consumer.stop()
	await asyncio.wait_for(consumer_task, timeout=1.0)

	assert consumer.setup_async_connection.await_count >= 1
	assert consumer._connection is None


@pytest.mark.asyncio
async def test_close_during_backoff_does_not_open_a_new_connection(amqp_consumer, monkeypatch):
	"""close() while the retry is backing off after a loss: the retry must not
	open a fresh connection that nobody closes."""
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_BlockingQueueIterator())
	monkeypatch.setattr(async_amqp, '_CONSUMER_RETRY_WAIT', wait_fixed(10))
	consumer_task = harness.start()
	await harness.wait_consuming()

	dropped = asyncio.Event()

	async def _close():
		dropped.set()
	harness.old_connection.close = AsyncMock(side_effect=_close)
	await harness.old_connection.close_callbacks(ConnectionError('Broken pipe'))
	await asyncio.wait_for(dropped.wait(), timeout=1.0)  # handles dropped, retry now in backoff
	await harness.consumer.close()
	await asyncio.wait_for(consumer_task, timeout=1.0)

	harness.consumer.setup_async_connection.assert_not_awaited()
	assert harness.consumer._connection is None


class _ExitRaisesIterator(_BlockingQueueIterator):
	"""Idle iterator whose async-with exit fails on the dead channel."""
	async def __aexit__(self, exc_type, exc, tb):
		raise ChannelInvalidStateError('No active transport in channel')


@pytest.mark.asyncio
async def test_iterator_exit_raising_during_local_close_does_not_reconnect(amqp_consumer, monkeypatch):
	"""A connection error from tearing the iterator down during close() must not
	turn the deliberate shutdown into a reconnect, nor wait out a retry backoff
	first: start_consumer returns at once."""
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_ExitRaisesIterator())
	monkeypatch.setattr(async_amqp, '_CONSUMER_RETRY_WAIT', wait_fixed(10))
	consumer_task = harness.start()
	await harness.wait_consuming()

	await harness.consumer.close()
	await asyncio.wait_for(consumer_task, timeout=1.0)

	harness.consumer.setup_async_connection.assert_not_awaited()


@pytest.mark.asyncio
async def test_close_while_connecting_closes_the_new_connection(amqp_consumer, monkeypatch):
	"""close() while prepare is still connecting: the connection assigned after
	the close must be closed, not left open by a consumer that never runs."""
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_BlockingQueueIterator())
	connected = asyncio.Event()

	connecting = asyncio.Event()

	async def _slow_reconnect():
		connecting.set()
		await connected.wait()
		harness.consumer._connection = harness.new_connection
	harness.consumer.setup_async_connection = AsyncMock(side_effect=_slow_reconnect)
	harness.consumer._connection = None
	harness.consumer._channel = None

	consumer_task = harness.start()
	await asyncio.wait_for(connecting.wait(), timeout=1.0)
	await harness.consumer.close()
	connected.set()
	await asyncio.wait_for(consumer_task, timeout=1.0)

	harness.consumer.setup_async_connection.assert_awaited_once()
	harness.new_connection.close.assert_awaited()
	harness.new_queue.iterator.assert_not_called()
	assert harness.consumer._connection is None


@pytest.mark.asyncio
async def test_failed_auto_declare_does_not_stop_the_instance(amqp_consumer, monkeypatch):
	"""A failed auto-declare raises MrsalAbortedSetup and drops the handles, but
	must not set the stop flag: a later start_consumer on the instance runs setup
	again instead of returning silently."""
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_BlockingQueueIterator())
	consumer = harness.consumer
	declare_ok = iter([False, True])

	async def _setup(**kwargs):
		consumer.auto_declare_ok = next(declare_ok)
		return harness.new_queue
	consumer._async_setup_exchange_and_queue = AsyncMock(side_effect=_setup)

	with pytest.raises(MrsalAbortedSetup):
		await harness.start()
	await asyncio.wait_for(harness.start(), timeout=1.0)

	assert consumer._async_setup_exchange_and_queue.await_count == 2
	harness.new_queue.iterator.assert_called_once()


@pytest.mark.asyncio
async def test_refused_credentials_raise_without_retry(monkeypatch):
	"""Refused credentials are a ConnectionError subclass, but retrying cannot fix
	them: start_consumer raises at once instead of retrying forever."""
	monkeypatch.setattr(async_amqp, '_CONSUMER_RETRY_WAIT', wait_fixed(10))
	consumer = MrsalAsyncAMQP(**SETUP_ARGS)
	consumer.setup_async_connection = AsyncMock(side_effect=AuthenticationError('ACCESS_REFUSED'))

	with pytest.raises(AuthenticationError):
		await asyncio.wait_for(consumer.start_consumer(
			queue_name='test_q', callback=AsyncMock(), routing_key='rk',
			exchange_name='test_x', exchange_type='direct', auto_ack=True, dlx_enable=False,
		), timeout=1.0)

	consumer.setup_async_connection.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('transport, expected', [(None, ChannelInvalidStateError), (MagicMock(), MrsalSetupError)])
async def test_channel_invalid_state_in_async_declare_is_a_loss_only_without_transport(amqp_consumer, transport, expected):
	"""ChannelInvalidStateError is a lost connection only when the connection has no
	transport. On a live connection it comes from a channel the broker closed, which
	is a topology failure."""
	amqp_consumer._connection.transport = transport
	amqp_consumer._channel.declare_queue.side_effect = ChannelInvalidStateError('Channel closed by RPC timeout')

	with pytest.raises(expected):
		await amqp_consumer._async_declare_queue(queue_name='q')


@pytest.mark.asyncio
async def test_topology_mismatch_aborts_instead_of_retrying(amqp_consumer, monkeypatch):
	"""A 406 on the DLX exchange closes the channel; the next declare on it raises
	ChannelInvalidStateError on a live connection. That must stay MrsalAbortedSetup,
	not become an endless retry of a permanent mismatch."""
	monkeypatch.setattr(async_amqp, '_CONSUMER_RETRY_WAIT', wait_none())
	consumer = amqp_consumer
	consumer.setup_async_connection = AsyncMock()
	consumer._channel.declare_exchange.side_effect = aiormq.exceptions.ChannelPreconditionFailed('PRECONDITION_FAILED')
	consumer._channel.declare_queue.side_effect = ChannelInvalidStateError('Channel closed by RPC timeout')

	with pytest.raises(MrsalAbortedSetup):
		await asyncio.wait_for(consumer.start_consumer(
			queue_name='test_q', callback=AsyncMock(), routing_key='rk',
			exchange_name='test_x', exchange_type='direct', dlx_enable=True,
		), timeout=1.0)

	consumer.setup_async_connection.assert_not_awaited()


@pytest.mark.asyncio
async def test_hanging_close_of_the_old_connection_does_not_block_the_rebuild(amqp_consumer, monkeypatch):
	"""Closing a half-dead connection can hang; the close is bounded so the
	consumer still rebuilds."""
	monkeypatch.setattr(config, 'CLOSE_TIMEOUT_SEC', 0.05)
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_BlockingQueueIterator())

	async def _hang():
		await asyncio.Event().wait()
	consumer_task = harness.start()
	await harness.wait_consuming()
	harness.old_connection.close = AsyncMock(side_effect=_hang)
	harness.old_channel.close = AsyncMock(side_effect=_hang)

	await harness.old_connection.close_callbacks(ConnectionError('Broken pipe'))
	await asyncio.wait_for(consumer_task, timeout=1.0)

	harness.old_connection.close.assert_awaited()
	harness.assert_rebuilt()


@pytest.mark.asyncio
async def test_failing_channel_close_still_closes_the_connection(amqp_consumer, caplog):
	"""A channel close that raises is logged at WARNING and must not leak the
	connection behind it."""
	connection = amqp_consumer._connection
	amqp_consumer._channel.close = AsyncMock(side_effect=ChannelInvalidStateError('No active transport in channel'))

	with caplog.at_level(logging.WARNING):
		await amqp_consumer.close()

	connection.close.assert_awaited_once()
	assert amqp_consumer._channel is None and amqp_consumer._connection is None
	assert any(
		r.levelno == logging.WARNING and 'Consumer channel close raised' in r.getMessage()
		for r in caplog.records
	)


def test_async_class_is_still_importable_from_subclass():
	"""#108 moved MrsalAsyncAMQP to mrsal.amqp.async_amqp; the old import path
	must keep resolving to the same class, and it keeps logging under the old
	logger name."""
	assert MrsalAsyncAMQP is async_amqp.MrsalAsyncAMQP
	assert 'MrsalAsyncAMQP' in subclass.__all__
	assert all(hasattr(subclass, name) for name in subclass.__all__)
	assert async_amqp.log.name == 'mrsal.amqp.subclass'


# --- DLX publish failure (#105) -----------------------------------------------

DLX_FAILURE_ARGS = {
	'processing_error': 'callback: boom',
	'original_exchange': 'test_x',
	'original_routing_key': 'test_route',
	'enable_retry_cycles': True,
	'retry_cycle_interval': 10,
	'max_retry_time_limit': 60,
	'dlx_exchange_name': None,
}


@pytest.mark.asyncio
@pytest.mark.parametrize('error', [
	BrokenPipeError(32, 'Broken pipe'),
	aiormq.exceptions.ConnectionClosed(320, 'CONNECTION_FORCED'),
	ChannelInvalidStateError('No active transport in channel'),
])
async def test_dlx_publish_connection_loss_leaves_message_unsettled(amqp_consumer, error):
	"""Connection gone mid-DLX-publish: neither ack nor reject, so the broker
	redelivers the message instead of it being dropped; the error is re-raised
	so the consume loop rebuilds even if only the DLX channel died."""
	consumer = amqp_consumer
	mock_message = AsyncMock(ack=AsyncMock(), reject=AsyncMock(), delivery_tag=5)

	with patch.object(consumer, '_handle_dlx_with_retry_cycle_async', AsyncMock(side_effect=error)):
		with pytest.raises(type(error)):
			await consumer._async_publish_to_dlx_with_retry_cycle(
				message=mock_message, properties=MagicMock(), **DLX_FAILURE_ARGS)

	mock_message.ack.assert_not_awaited()
	mock_message.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_dlx_publish_broker_rejection_still_rejects(amqp_consumer):
	"""A non-connection DLX failure (e.g. broker nack) keeps the old behaviour:
	reject(requeue=False)."""
	consumer = amqp_consumer
	mock_message = AsyncMock(ack=AsyncMock(), reject=AsyncMock(), delivery_tag=5)

	with patch.object(consumer, '_handle_dlx_with_retry_cycle_async', AsyncMock(side_effect=DeliveryError(None, None))):
		await consumer._async_publish_to_dlx_with_retry_cycle(
			message=mock_message, properties=MagicMock(), **DLX_FAILURE_ARGS)

	mock_message.ack.assert_not_awaited()
	mock_message.reject.assert_awaited_once_with(requeue=False)


# --- DLX publish timeout (#105, M2) -------------------------------------------

@pytest.mark.asyncio
async def test_dlx_publish_never_confirming_times_out(amqp_consumer):
	"""_publish_to_dlx must not wait forever for a publisher confirm."""
	consumer = amqp_consumer
	consumer.dlx_publish_timeout = 0.05
	async def _never_confirms(*args, **kwargs):
		await asyncio.Event().wait()

	dlx_exchange = AsyncMock()
	dlx_exchange.publish = AsyncMock(side_effect=_never_confirms)
	dlx_channel = AsyncMock(is_closed=False)
	dlx_channel.get_exchange = AsyncMock(return_value=dlx_exchange)
	consumer._dlx_publish_channel = dlx_channel

	# The outer wait_for is only a guard against hanging the suite; the
	# timeout must come from dlx_publish_timeout, i.e. well before it.
	start = time.monotonic()
	with pytest.raises(MrsalDLXPublishTimeout):
		await asyncio.wait_for(
			consumer._publish_to_dlx(dlx_exchange='x.dlx', routing_key='rk', body=b'{}', properties={}),
			timeout=1.0,
		)
	assert time.monotonic() - start < 0.5


@pytest.mark.asyncio
async def test_hung_dlx_publish_rejects_delivery_and_next_message_is_processed(amqp_consumer):
	"""Spec M2 acceptance: the DLX publish never confirms -> the delivery is
	rejected within the timeout and the next message is still processed."""
	consumer = amqp_consumer
	consumer.dlx_publish_timeout = 0.05

	failing = AsyncMock(body=b'{"n": 1}', ack=AsyncMock(), reject=AsyncMock())
	failing.configure_mock(app_id="test_app", message_id="m1", headers=None, redelivered=False, routing_key="rk", delivery_tag=1)
	healthy = AsyncMock(body=b'{"n": 2}', ack=AsyncMock(), reject=AsyncMock())
	healthy.configure_mock(app_id="test_app", message_id="m2", headers=None, redelivered=False, routing_key="rk", delivery_tag=2)
	mock_queue, _ = make_queue_with_messages([failing, healthy])
	consumer._channel.declare_queue.return_value = mock_queue

	async def callback(message, properties, body):
		if body == b'{"n": 1}':
			raise RuntimeError("boom")

	# The DLX publish hangs forever: no confirm ever arrives.
	async def _hang(*args, **kwargs):
		await asyncio.Event().wait()

	with patch.object(consumer, '_ensure_dlx_publish_channel', AsyncMock(side_effect=_hang)):
		await asyncio.wait_for(consumer.start_consumer(
			queue_name='test_q',
			callback=callback,
			routing_key='test_route',
			exchange_name='test_x',
			exchange_type='direct',
			auto_ack=False,
		), timeout=2.0)

	failing.reject.assert_awaited_once_with(requeue=False)
	failing.ack.assert_not_awaited()
	healthy.ack.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('callback_fails', [False, True])
async def test_on_consume_fires_after_delivery_is_settled(amqp_consumer, callback_fails):
	"""Hosts use on_consume as the 'delivery settled' signal for stall detection,
	so it must fire after the ack / DLX reject, never before."""
	consumer = amqp_consumer
	consumer.dlx_publish_timeout = 0.05
	events: list[str] = []
	consumer.set_metrics_hooks(MetricsHooks(on_consume=lambda ok, duration: events.append('on_consume')))

	message = AsyncMock(body=b'{}')
	message.ack = AsyncMock(side_effect=lambda *a, **k: events.append('ack'))
	message.reject = AsyncMock(side_effect=lambda *a, **k: events.append('reject'))
	message.configure_mock(app_id="test_app", message_id="m1", headers=None, redelivered=False, routing_key="rk", delivery_tag=1)
	mock_queue, _ = make_queue_with_messages([message])
	consumer._channel.declare_queue.return_value = mock_queue

	async def callback(message, properties, body):
		if callback_fails:
			raise RuntimeError("boom")

	async def _hang(*args, **kwargs):
		await asyncio.Event().wait()

	with patch.object(consumer, '_ensure_dlx_publish_channel', AsyncMock(side_effect=_hang)):
		await asyncio.wait_for(consumer.start_consumer(
			queue_name='test_q',
			callback=callback,
			routing_key='test_route',
			exchange_name='test_x',
			exchange_type='direct',
			auto_ack=False,
		), timeout=2.0)

	assert events == (['reject', 'on_consume'] if callback_fails else ['ack', 'on_consume'])


@pytest.mark.asyncio
async def test_dlx_timeout_drops_the_channel_and_next_publish_opens_a_fresh_one(amqp_consumer):
	"""107 review: a stuck DLX channel must not be reused for the next failure."""
	consumer = amqp_consumer
	consumer.dlx_publish_timeout = 0.05

	async def _never_confirms(*args, **kwargs):
		await asyncio.Event().wait()

	stuck_exchange = AsyncMock()
	stuck_exchange.publish = AsyncMock(side_effect=_never_confirms)
	stuck_channel = AsyncMock(is_closed=False)
	stuck_channel.get_exchange = AsyncMock(return_value=stuck_exchange)
	consumer._dlx_publish_channel = stuck_channel

	with pytest.raises(MrsalDLXPublishTimeout):
		await consumer._publish_to_dlx(dlx_exchange='x.dlx', routing_key='rk', body=b'{}', properties={})

	stuck_channel.close.assert_awaited_once()
	assert consumer._dlx_publish_channel is None

	fresh_exchange = AsyncMock()
	fresh_channel = AsyncMock(is_closed=False)
	fresh_channel.get_exchange = AsyncMock(return_value=fresh_exchange)
	consumer._connection.channel = AsyncMock(return_value=fresh_channel)

	await consumer._publish_to_dlx(dlx_exchange='x.dlx', routing_key='rk', body=b'{}', properties={})

	consumer._connection.channel.assert_awaited_once_with(publisher_confirms=True)
	fresh_exchange.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_connection_error_from_dlx_publish_is_not_treated_as_a_timeout(amqp_consumer):
	"""A ConnectionError inside the timeout wrapper comes out as itself, so the
	connection-loss branch (re-raise, no reject) handles it, and the channel is
	not dropped as if it were stuck."""
	consumer = amqp_consumer
	dlx_exchange = AsyncMock()
	dlx_exchange.publish = AsyncMock(side_effect=ConnectionError('Broken pipe'))
	dlx_channel = AsyncMock(is_closed=False)
	dlx_channel.get_exchange = AsyncMock(return_value=dlx_exchange)
	consumer._dlx_publish_channel = dlx_channel

	with pytest.raises(ConnectionError):
		await consumer._publish_to_dlx(dlx_exchange='x.dlx', routing_key='rk', body=b'{}', properties={})

	assert consumer._dlx_publish_channel is dlx_channel
	dlx_channel.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_socket_timeout_from_ack_is_not_logged_as_a_dlx_publish_timeout(amqp_consumer, caplog):
	"""107 review: on Python 3.11+ asyncio.TimeoutError is the builtin TimeoutError;
	a socket timeout from ack() must take the generic branch, not the DLX-timeout one."""
	consumer = amqp_consumer
	mock_message = AsyncMock(reject=AsyncMock(), delivery_tag=5)
	mock_message.ack = AsyncMock(side_effect=TimeoutError('socket timed out'))

	with patch.object(consumer, '_handle_dlx_with_retry_cycle_async', AsyncMock()):
		with caplog.at_level(logging.ERROR):
			await consumer._async_publish_to_dlx_with_retry_cycle(
				message=mock_message, properties=MagicMock(), **DLX_FAILURE_ARGS)

	mock_message.reject.assert_awaited_once_with(requeue=False)
	assert "DLX publish timed out" not in caplog.text
	assert "Failed to send message to DLX" in caplog.text


@pytest.mark.asyncio
async def test_on_consume_fires_when_delivery_is_left_unsettled_on_connection_loss(amqp_consumer, monkeypatch):
	"""on_consume also fires on the connection-loss path, where mrsal deliberately
	leaves the delivery unsettled for broker redelivery and rebuilds."""
	events: list[str] = []
	amqp_consumer.set_metrics_hooks(MetricsHooks(on_consume=lambda ok, duration: events.append('on_consume')))

	message = AsyncMock(body=b'{}')
	message.ack = AsyncMock(side_effect=lambda *a, **k: events.append('ack'))
	message.reject = AsyncMock(side_effect=lambda *a, **k: events.append('reject'))
	message.configure_mock(app_id="a", message_id="m1", headers=None, redelivered=False, routing_key="rk", delivery_tag=1)
	harness = _RebuildHarness(amqp_consumer, monkeypatch, old_iterator=_QueueThenBlock([message]))

	async def failing_callback(message, properties, body):
		raise RuntimeError("boom")

	with patch.object(amqp_consumer, '_ensure_dlx_publish_channel', AsyncMock(side_effect=ConnectionError('Broken pipe'))):
		await asyncio.wait_for(harness.start(callback=failing_callback, auto_ack=False, dlx_enable=True), timeout=1.0)

	assert events == ['on_consume']
	harness.assert_rebuilt()
