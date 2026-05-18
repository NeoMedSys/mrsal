import asyncio
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from mrsal.amqp.subclass import MrsalAsyncAMQP
from mrsal.config import AioPikaAttributes
from mrsal.exceptions import MrsalAbortedSetup
from pydantic import ValidationError

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
        mock_channel.is_closed = False
        mock_connection = AsyncMock()
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

    with patch('mrsal.amqp.subclass.connect_robust',
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
        self.iterator_call_kwargs = None
        self.close_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        await self._closed.wait()
        raise StopAsyncIteration

    async def close(self):
        self.close_calls += 1
        self._closed.set()


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

    # Let the consumer reach `async for message in it` and block on __anext__.
    await asyncio.sleep(0.05)
    assert not consumer_task.done(), "consumer should be blocked on idle iterator"

    await consumer.stop()
    await asyncio.wait_for(consumer_task, timeout=1.0)

    callback.assert_not_called()
    assert blocking_it.close_calls >= 1
