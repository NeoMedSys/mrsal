"""Milestone 1 of the in-memory test broker (Issue 4): round-trip core.

Covers publish -> route -> validate -> callback -> ack/drop for both the sync
(``MrsalBlockingAMQP``) and async (``MrsalAsyncAMQP``) consumers, plus topic
routing. DLX dead-lettering and the retry-cycle clock land in the follow-up.
"""
import pytest
from pydantic.dataclasses import dataclass

from mrsal.amqp.subclass import MrsalBlockingAMQP, MrsalAsyncAMQP
from mrsal.exceptions import MrsalAbortedSetup
from mrsal.testing import TestMrsalBroker, TestMrsalAsyncBroker, topic_matches


SYNC_ARGS = {
	"host": "localhost",
	"port": 5672,
	"credentials": ("guest", "guest"),
	"virtual_host": "/",
	"ssl": False,
	"prefetch_count": 1,
}

ASYNC_ARGS = {
	"host": "localhost",
	"port": 5672,
	"credentials": ("guest", "guest"),
	"virtual_host": "/",
	"ssl": False,
	"prefetch_count": 1,
}


@dataclass
class OrderEvent:
	order_id: str
	amount: float


# -- topic routing -----------------------------------------------------------

def test_topic_matches_wildcards():
	assert topic_matches("orders.*", "orders.new")
	assert not topic_matches("orders.*", "orders.new.eu")
	assert topic_matches("orders.#", "orders.new.eu")
	assert topic_matches("orders.#", "orders")
	assert not topic_matches("orders.new", "orders.old")


# -- sync --------------------------------------------------------------------

def test_sync_roundtrip_with_payload_model():
	received = []

	def handle(method_frame, properties, body):
		received.append(body)

	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	with TestMrsalBroker(consumer) as br:
		br.register_consumer(
			queue_name="orders",
			exchange_name="orders.x",
			exchange_type="direct",
			routing_key="orders.new",
			callback=handle,
			payload_model=OrderEvent,
			dlx_enable=False,
			enable_retry_cycles=False,
		)
		br.publish(
			{"order_id": "abc", "amount": 10.0},
			exchange="orders.x",
			routing_key="orders.new",
		)

	assert len(received) == 1
	assert isinstance(received[0], OrderEvent)
	assert received[0].order_id == "abc"
	assert received[0].amount == 10.0
	# Successful processing acks the delivery -> queue is empty.
	assert br.message_count("orders") == 0


def test_sync_roundtrip_raw_body_no_model():
	received = []

	def handle(method_frame, properties, body):
		received.append(body)

	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	with TestMrsalBroker(consumer) as br:
		br.register_consumer(
			queue_name="raw",
			exchange_name="raw.x",
			exchange_type="direct",
			routing_key="raw.k",
			callback=handle,
			dlx_enable=False,
			enable_retry_cycles=False,
		)
		br.publish(b"hello world", exchange="raw.x", routing_key="raw.k")

	assert received == [b"hello world"]
	assert br.message_count("raw") == 0


def test_sync_validation_failure_dropped_without_dlx():
	received = []

	def handle(method_frame, properties, body):
		received.append(body)

	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	with TestMrsalBroker(consumer) as br:
		br.register_consumer(
			queue_name="orders",
			exchange_name="orders.x",
			exchange_type="direct",
			routing_key="orders.new",
			callback=handle,
			payload_model=OrderEvent,
			dlx_enable=False,
			enable_retry_cycles=False,
		)
		# Missing required fields -> ValidationError -> callback never runs.
		br.publish({"nonsense": "data"}, exchange="orders.x", routing_key="orders.new")

	assert received == []
	assert br.message_count("orders") == 0


def test_sync_callback_exception_dropped_without_dlx():
	def boom(method_frame, properties, body):
		raise RuntimeError("deliberate failure")

	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	with TestMrsalBroker(consumer) as br:
		br.register_consumer(
			queue_name="jobs",
			exchange_name="jobs.x",
			exchange_type="direct",
			routing_key="jobs.k",
			callback=boom,
			dlx_enable=False,
			enable_retry_cycles=False,
		)
		br.publish(b"payload", exchange="jobs.x", routing_key="jobs.k")

	assert br.message_count("jobs") == 0


def test_sync_unrouted_publish_lands_nowhere():
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	with TestMrsalBroker(consumer) as br:
		br.register_consumer(
			queue_name="orders",
			exchange_name="orders.x",
			exchange_type="direct",
			routing_key="orders.new",
			callback=lambda *a: None,
			dlx_enable=False,
			enable_retry_cycles=False,
		)
		# Wrong routing key: nothing is bound for it.
		br.publish(b"x", exchange="orders.x", routing_key="orders.other")

	assert br.message_count("orders") == 0


# -- harness guard rails -----------------------------------------------------

def test_register_consumer_rejects_threaded():
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	with TestMrsalBroker(consumer) as br:
		with pytest.raises(ValueError, match="threaded=True is not supported"):
			br.register_consumer(
				queue_name="q", exchange_name="e", exchange_type="direct",
				routing_key="r", callback=lambda *a: None, threaded=True,
			)


def test_register_consumer_ignores_loop_only_kwargs():
	received = []
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	with TestMrsalBroker(consumer) as br:
		# inactivity_timeout / max_workers are valid start_consumer kwargs but
		# no-ops for inline delivery -- accepted, not rejected.
		br.register_consumer(
			queue_name="q", exchange_name="e", exchange_type="direct",
			routing_key="r", callback=lambda *a: received.append(1),
			dlx_enable=False, enable_retry_cycles=False,
			inactivity_timeout=1, max_workers=4,
		)
		br.publish(b"x", exchange="e", routing_key="r")
	assert received == [1]


def test_register_consumer_rejects_unknown_kwarg():
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	with TestMrsalBroker(consumer) as br:
		with pytest.raises(TypeError, match="unexpected keyword"):
			br.register_consumer(
				queue_name="q", exchange_name="e", exchange_type="direct",
				routing_key="r", callback=lambda *a: None, bogus_param=1,
			)


def test_exit_restores_connection_and_clears_fake_channels():
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	assert consumer._connection is None
	with TestMrsalBroker(consumer) as br:
		br.register_consumer(
			queue_name="q", exchange_name="e", exchange_type="direct",
			routing_key="r", callback=lambda *a: None,
			dlx_enable=False, enable_retry_cycles=False,
		)
		assert consumer._connection is not None  # swapped to the in-memory fake
		assert consumer._consumer_channel is not None
	# On exit the original connection is restored and fake channels are cleared.
	assert consumer._connection is None
	assert consumer._consumer_channel is None


@pytest.mark.asyncio
async def test_async_register_consumer_rejects_max_concurrent_tasks():
	consumer = MrsalAsyncAMQP(**ASYNC_ARGS)
	async with TestMrsalAsyncBroker(consumer) as br:
		with pytest.raises(ValueError, match="max_concurrent_tasks is not supported"):
			await br.register_consumer(
				queue_name="q", exchange_name="e", exchange_type="direct",
				routing_key="r", callback=lambda *a: None, max_concurrent_tasks=4,
			)


@pytest.mark.asyncio
async def test_async_auto_declare_false_raises_clear_error():
	consumer = MrsalAsyncAMQP(**ASYNC_ARGS)
	async with TestMrsalAsyncBroker(consumer) as br:
		with pytest.raises(MrsalAbortedSetup, match="requires auto_declare=True"):
			await br.register_consumer(
				queue_name="q", exchange_name="e", exchange_type="direct",
				routing_key="r", callback=lambda *a: None, auto_declare=False,
			)


# -- async -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_roundtrip_with_payload_model():
	received = []

	async def handle(message, properties, body):
		received.append(body)

	consumer = MrsalAsyncAMQP(**ASYNC_ARGS)
	async with TestMrsalAsyncBroker(consumer) as br:
		await br.register_consumer(
			queue_name="orders",
			exchange_name="orders.x",
			exchange_type="direct",
			routing_key="orders.new",
			callback=handle,
			payload_model=OrderEvent,
			dlx_enable=False,
			enable_retry_cycles=False,
		)
		await br.publish(
			{"order_id": "xyz", "amount": 5.5},
			exchange="orders.x",
			routing_key="orders.new",
		)

	assert len(received) == 1
	assert isinstance(received[0], OrderEvent)
	assert received[0].order_id == "xyz"
	assert br.message_count("orders") == 0


@pytest.mark.asyncio
async def test_async_validation_failure_dropped_without_dlx():
	received = []

	async def handle(message, properties, body):
		received.append(body)

	consumer = MrsalAsyncAMQP(**ASYNC_ARGS)
	async with TestMrsalAsyncBroker(consumer) as br:
		await br.register_consumer(
			queue_name="orders",
			exchange_name="orders.x",
			exchange_type="direct",
			routing_key="orders.new",
			callback=handle,
			payload_model=OrderEvent,
			dlx_enable=False,
			enable_retry_cycles=False,
		)
		await br.publish({"nope": 1}, exchange="orders.x", routing_key="orders.new")

	assert received == []
	assert br.message_count("orders") == 0
