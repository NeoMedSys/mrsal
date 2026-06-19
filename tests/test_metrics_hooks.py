"""Push-based metrics hooks (Issue 3b): on_publish / on_consume / on_retry / on_dlx_final.

Exercised against the in-memory broker (Issue 4) so the hooks fire on the real
consume / DLX / publish machinery without docker. Mirrors sonic's
``types.MetricsHooks`` semantics: on_consume fires once per delivery with
success=False on validation or callback failure; on_retry / on_dlx_final map to
the ``.retry`` vs terminal ``.dlx`` publish.
"""
import types

import pytest

from mrsal import config
from mrsal.amqp.subclass import MrsalBlockingAMQP, MrsalAsyncAMQP, MrsalBlockingPublisher, MrsalBlockingPublisherPool
from mrsal.metrics import MetricsHooks
from mrsal.testing import InMemoryBroker, TestMrsalBroker, TestMrsalAsyncBroker
from mrsal.testing._sync import InMemoryBlockingConnection
from pika.exceptions import UnroutableError
from pydantic import BaseModel


SYNC_ARGS = {
	"host": "localhost",
	"port": 5672,
	"credentials": ("guest", "guest"),
	"virtual_host": "/",
	"ssl": False,
	"prefetch_count": 1,
}

ASYNC_ARGS = dict(SYNC_ARGS)


class Recorder:
	"""Collects hook invocations as ``(name, args)`` tuples."""

	def __init__(self):
		self.calls: list[tuple] = []

	def hook(self, name):
		def _record(*args):
			self.calls.append((name, args))
		return _record

	def of(self, name):
		return [args for (n, args) in self.calls if n == name]


def _hooks(rec: Recorder) -> MetricsHooks:
	return MetricsHooks(
		on_publish=rec.hook("on_publish"),
		on_consume=rec.hook("on_consume"),
		on_retry=rec.hook("on_retry"),
		on_dlx_final=rec.hook("on_dlx_final"),
	)


def _register_sync(br, callback, **overrides):
	kwargs = dict(
		queue_name="q",
		exchange_name="ex",
		exchange_type="direct",
		routing_key="rk",
		callback=callback,
		use_quorum_queues=False,
		dlx_enable=False,
		enable_retry_cycles=False,
	)
	kwargs.update(overrides)
	br.register_consumer(**kwargs)


# -- on_consume --------------------------------------------------------------

def test_on_consume_success():
	rec = Recorder()
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_metrics_hooks(_hooks(rec))
	with TestMrsalBroker(consumer) as br:
		_register_sync(br, lambda mf, props, body: None)
		br.publish(b"ok", exchange="ex", routing_key="rk")

	consumes = rec.of("on_consume")
	assert len(consumes) == 1
	success, duration = consumes[0]
	assert success is True
	assert isinstance(duration, float) and duration >= 0


def test_on_consume_callback_failure():
	rec = Recorder()
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_metrics_hooks(_hooks(rec))

	def boom(mf, props, body):
		raise RuntimeError("deliberate failure")

	with TestMrsalBroker(consumer) as br:
		_register_sync(br, boom)
		br.publish(b"bad", exchange="ex", routing_key="rk")

	consumes = rec.of("on_consume")
	assert len(consumes) == 1
	assert consumes[0][0] is False


def test_on_consume_validation_failure():
	rec = Recorder()
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_metrics_hooks(_hooks(rec))

	class Schema(BaseModel):
		order_id: str

	# Callback must never run when validation fails.
	def should_not_run(mf, props, body):
		raise AssertionError("callback ran despite validation failure")

	with TestMrsalBroker(consumer) as br:
		_register_sync(br, should_not_run, payload_model=Schema)
		br.publish(b"not json at all", exchange="ex", routing_key="rk")

	consumes = rec.of("on_consume")
	assert len(consumes) == 1
	assert consumes[0][0] is False


def test_on_consume_fires_once_per_delivery():
	rec = Recorder()
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_metrics_hooks(_hooks(rec))
	with TestMrsalBroker(consumer) as br:
		_register_sync(br, lambda mf, props, body: None)
		br.publish(b"one", exchange="ex", routing_key="rk")
		br.publish(b"two", exchange="ex", routing_key="rk")

	assert len(rec.of("on_consume")) == 2


def test_on_consume_fires_from_threaded_path():
	"""The harness can't model threads, so drive _process_single_message directly
	with threaded=True to lock the on_consume contract on the executor path."""
	rec = Recorder()
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_metrics_hooks(_hooks(rec))

	class _Chan:
		def __init__(self):
			self.acked = []

		def basic_ack(self, delivery_tag):
			self.acked.append(delivery_tag)

		def basic_nack(self, delivery_tag, requeue=False):
			pass

	class _Conn:
		is_open = True

		def add_callback_threadsafe(self, cb):
			cb()  # single-threaded test: run the scheduled ack inline

	chan = _Chan()
	consumer._consumer_channel = chan
	consumer._connection = _Conn()

	runtime_config = {
		"auto_ack": False, "threaded": True, "callback": lambda mf, p, b: None,
		"callback_args": None, "payload_model": None, "dlx_enable": False,
		"enable_retry_cycles": False, "exchange_name": "ex", "routing_key": "rk",
		"retry_cycle_interval": 1, "max_retry_time_limit": 1, "dlx_exchange_name": None,
		"dlx_routing_key": None, "retry_backoff": "fixed", "retry_backoff_max": 1,
		"queue_name": "q",
	}
	method_frame = types.SimpleNamespace(delivery_tag=1, routing_key="rk")
	properties = types.SimpleNamespace(app_id="a", message_id="m", headers={})

	consumer._process_single_message(method_frame, properties, b"x", runtime_config)

	assert chan.acked == [1]
	consumes = rec.of("on_consume")
	assert len(consumes) == 1
	assert consumes[0][0] is True


# -- on_retry / on_dlx_final -------------------------------------------------

def test_on_dlx_final_on_exhaustion():
	rec = Recorder()
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_metrics_hooks(_hooks(rec))

	def boom(mf, props, body):
		raise RuntimeError("deliberate failure")

	with TestMrsalBroker(consumer) as br:
		# max_retry_time_limit=0 -> first failure parks straight in the terminal .dlx.
		_register_sync(
			br, boom,
			dlx_enable=True,
			enable_retry_cycles=True,
			max_retry_time_limit=0,
			retry_cycle_interval=1,
		)
		br.publish(b"to be parked", exchange="ex", routing_key="rk")

	finals = rec.of("on_dlx_final")
	assert len(finals) == 1
	assert finals[0] == (f"q{config.DLX_SUFFIX}",)
	# Same delivery also reports a failed consume, and never a retry.
	assert rec.of("on_consume")[0][0] is False
	assert rec.of("on_retry") == []


def test_on_retry_on_cycle():
	rec = Recorder()
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_metrics_hooks(_hooks(rec))

	def boom(mf, props, body):
		raise RuntimeError("deliberate failure")

	with TestMrsalBroker(consumer) as br:
		# Fixed backoff: delay is the flat .retry queue TTL (retry_cycle_interval min).
		_register_sync(
			br, boom,
			dlx_enable=True,
			enable_retry_cycles=True,
			retry_backoff="fixed",
			retry_cycle_interval=10,
			max_retry_time_limit=8 * 60,
		)
		br.publish(b"cycle me", exchange="ex", routing_key="rk")

	retries = rec.of("on_retry")
	assert len(retries) == 1
	cycle, delay_s = retries[0]
	assert cycle == 1
	assert delay_s == pytest.approx(10 * 60)
	assert rec.of("on_dlx_final") == []


# -- on_publish --------------------------------------------------------------

def test_on_publish_success():
	rec = Recorder()
	broker = InMemoryBroker()
	publisher = MrsalBlockingAMQP(**SYNC_ARGS)
	publisher._connection = InMemoryBlockingConnection(broker)
	publisher.set_metrics_hooks(_hooks(rec))

	publisher.publish_message(
		exchange_name="ex",
		routing_key="rk",
		message=b"hi",
		exchange_type="direct",
		queue_name="q",
		auto_declare=True,
	)

	publishes = rec.of("on_publish")
	assert len(publishes) == 1
	success, duration = publishes[0]
	assert success is True
	assert isinstance(duration, float) and duration >= 0


def test_on_publish_batch_fires_once_per_call():
	rec = Recorder()
	broker = InMemoryBroker()
	publisher = MrsalBlockingAMQP(**SYNC_ARGS)
	publisher._connection = InMemoryBlockingConnection(broker)
	publisher.set_metrics_hooks(_hooks(rec))

	collection = {
		"app_1": {"message": b"m1", "routing_key": "rk1", "queue_name": "q1", "exchange_type": "direct", "exchange_name": "ex1"},
		"app_2": {"message": b"m2", "routing_key": "rk2", "queue_name": "q2", "exchange_type": "direct", "exchange_name": "ex2"},
	}
	publisher.publish_messages(collection, auto_declare=True)

	# One call for the whole batch (not one per message, not one per retry attempt).
	publishes = rec.of("on_publish")
	assert len(publishes) == 1
	assert publishes[0][0] is True


def test_on_publish_failure_unroutable():
	rec = Recorder()
	broker = InMemoryBroker()
	# Exchange exists but nothing is bound, so a mandatory publish is unroutable.
	broker.declare_exchange("ex", exchange_type="direct")
	publisher = MrsalBlockingPublisher(**SYNC_ARGS)
	publisher._connection = InMemoryBlockingConnection(broker)
	publisher.set_metrics_hooks(_hooks(rec))

	with pytest.raises(UnroutableError):
		publisher.publish(
			exchange_name="ex",
			routing_key="rk",
			message=b"hi",
			exchange_type="direct",
			queue_name="q",
			auto_declare=False,
			passive=False,
		)

	publishes = rec.of("on_publish")
	assert len(publishes) == 1
	assert publishes[0][0] is False


# -- contract: unset hooks, partial hooks, exception safety ------------------

def test_unset_hooks_no_overhead_no_error():
	"""No hooks installed -> consume runs unchanged."""
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	seen = []
	with TestMrsalBroker(consumer) as br:
		_register_sync(br, lambda mf, props, body: seen.append(body))
		br.publish(b"plain", exchange="ex", routing_key="rk")
	assert seen == [b"plain"]


def test_set_metrics_hooks_none_clears():
	rec = Recorder()
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_metrics_hooks(_hooks(rec))
	consumer.set_metrics_hooks(None)
	with TestMrsalBroker(consumer) as br:
		_register_sync(br, lambda mf, props, body: None)
		br.publish(b"ok", exchange="ex", routing_key="rk")
	assert rec.calls == []


def test_consume_hook_exception_is_swallowed():
	"""A raising on_consume must not break delivery (consume-path guard)."""
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)

	def angry(success, duration):
		raise RuntimeError("hook blew up")

	consumer.set_metrics_hooks(MetricsHooks(on_consume=angry))
	seen = []
	with TestMrsalBroker(consumer) as br:
		_register_sync(br, lambda mf, props, body: seen.append(body))
		br.publish(b"resilient", exchange="ex", routing_key="rk")
	assert seen == [b"resilient"]


# -- pool cascade ------------------------------------------------------------

def test_pool_cascades_hooks_to_publishers():
	hooks = MetricsHooks(on_publish=lambda success, duration: None)
	pool = MrsalBlockingPublisherPool(size=1, **SYNC_ARGS)
	pool.set_metrics_hooks(hooks)
	with pool.acquire() as pub:
		assert pub._metrics_hooks is hooks


# -- async -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_on_consume_success():
	rec = Recorder()

	async def handler(message, properties, body):
		return None

	consumer = MrsalAsyncAMQP(**ASYNC_ARGS)
	consumer.set_metrics_hooks(_hooks(rec))
	async with TestMrsalAsyncBroker(consumer) as br:
		await br.register_consumer(
			queue_name="q",
			exchange_name="ex",
			exchange_type="direct",
			routing_key="rk",
			callback=handler,
			use_quorum_queues=False,
			dlx_enable=False,
			enable_retry_cycles=False,
		)
		await br.publish(b"async ok", exchange="ex", routing_key="rk")

	consumes = rec.of("on_consume")
	assert len(consumes) == 1
	assert consumes[0][0] is True


@pytest.mark.asyncio
async def test_async_on_dlx_final_on_exhaustion():
	rec = Recorder()

	async def boom(message, properties, body):
		raise RuntimeError("deliberate async failure")

	consumer = MrsalAsyncAMQP(**ASYNC_ARGS)
	consumer.set_metrics_hooks(_hooks(rec))
	async with TestMrsalAsyncBroker(consumer) as br:
		await br.register_consumer(
			queue_name="q",
			exchange_name="ex",
			exchange_type="direct",
			routing_key="rk",
			callback=boom,
			use_quorum_queues=False,
			dlx_enable=True,
			enable_retry_cycles=True,
			max_retry_time_limit=0,
			retry_cycle_interval=1,
		)
		await br.publish(b"async parked", exchange="ex", routing_key="rk")

	assert rec.of("on_dlx_final") == [(f"q{config.DLX_SUFFIX}",)]
	assert rec.of("on_consume")[0][0] is False
