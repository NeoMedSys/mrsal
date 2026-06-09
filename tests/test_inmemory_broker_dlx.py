"""Milestone 2 of the in-memory test broker (Issue 4): DLX + retry cycles.

Exercises mrsal's real dead-letter machinery against the in-memory broker:
terminal parking on exhaustion, the modeled-clock ``.retry`` -> origin cycle,
and queue-level dead-lettering on nack without retry cycles.

``test_custom_dlx_routing_key_*`` is the in-memory port of
``tests/integration/test_dlx_retry_cycle.py`` -- same scenario and assertions,
no docker. The integration version stays as the periodic real-broker check.
"""
import json

import pytest

from mrsal import config
from mrsal.amqp.subclass import MrsalBlockingAMQP, MrsalAsyncAMQP
from mrsal.testing import TestMrsalBroker, TestMrsalAsyncBroker


SYNC_ARGS = {
	"host": "localhost",
	"port": 5672,
	"credentials": ("guest", "guest"),
	"virtual_host": "/",
	"ssl": False,
	"prefetch_count": 1,
}

ASYNC_ARGS = dict(SYNC_ARGS)


def _register_failing_sync(br, **overrides):
	def failing(method_frame, properties, body):
		raise RuntimeError("deliberate failure to drive the DLX path")

	kwargs = dict(
		queue_name="q",
		exchange_name="ex",
		exchange_type="direct",
		routing_key="rk",
		callback=failing,
		use_quorum_queues=False,
		dlx_enable=True,
		enable_retry_cycles=True,
	)
	kwargs.update(overrides)
	br.register_consumer(**kwargs)


def test_sync_terminal_dlx_on_exhaustion():
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	with TestMrsalBroker(consumer) as br:
		# max_retry_time_limit=0 makes the first failure already retry-exhausted,
		# so it parks straight in the terminal .dlx queue.
		_register_failing_sync(br, max_retry_time_limit=0, retry_cycle_interval=1)
		br.publish(b"to be DLX'd", exchange="ex", routing_key="rk")

	dlx_queue = f"q{config.DLX_SUFFIX}"
	retry_queue = f"q{config.RETRY_SUFFIX}"
	assert br.message_count("q") == 0
	assert br.message_count(retry_queue) == 0
	assert br.message_count(dlx_queue) == 1

	parked = br.messages(dlx_queue)[0]
	assert parked.body == b"to be DLX'd"
	assert parked.routing_key == "rk"
	assert parked.headers.get("x-retry-exhausted") is True
	assert parked.headers.get("x-cycle-count") == 1
	assert "x-processing-error" in parked.headers


def test_sync_retry_cycle_advances_through_modeled_clock():
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	with TestMrsalBroker(consumer) as br:
		# Fixed backoff: the .retry queue TTL (retry_cycle_interval minutes) drives
		# re-delivery, no per-message jitter -> deterministic to advance past.
		_register_failing_sync(
			br,
			retry_backoff="fixed",
			retry_cycle_interval=10,
			max_retry_time_limit=8 * 60,
		)
		br.publish(b"cycle me", exchange="ex", routing_key="rk")

		retry_queue = f"q{config.RETRY_SUFFIX}"
		# First failure parks the message in .retry (not yet exhausted).
		assert br.message_count("q") == 0
		assert br.message_count(retry_queue) == 1
		first = br.messages(retry_queue)[0]
		assert first.headers.get("x-cycle-count") == 1
		assert first.headers.get("x-retry-exhausted") is False

		# Not yet due.
		br.advance(minutes=9)
		assert br.message_count(retry_queue) == 1

		# TTL elapses -> dead-letters back to origin -> handler fails again ->
		# re-published to .retry with an incremented cycle count.
		br.advance(minutes=1)
		assert br.message_count(retry_queue) == 1
		second = br.messages(retry_queue)[0]
		assert second.headers.get("x-cycle-count") == 2


def test_sync_dlx_without_retry_cycles_nacks_to_dlx():
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	with TestMrsalBroker(consumer) as br:
		# No retry cycles: the failure path is basic_nack(requeue=False), and the
		# broker dead-letters via the origin queue's x-dead-letter-* arguments.
		_register_failing_sync(br, enable_retry_cycles=False)
		br.publish(b"straight to dlx", exchange="ex", routing_key="rk")

	dlx_queue = f"q{config.DLX_SUFFIX}"
	assert br.message_count("q") == 0
	assert br.message_count(dlx_queue) == 1
	assert br.messages(dlx_queue)[0].body == b"straight to dlx"


def test_custom_dlx_routing_key_routes_to_dlx_queue():
	"""In-memory port of integration test_failed_callback_routes_to_dlx_with_custom_routing_key."""
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	payload = json.dumps({"id": 1, "msg": "to be DLX'd"}).encode("utf-8")
	with TestMrsalBroker(consumer) as br:
		def failing_callback(method_frame, properties, body):
			raise RuntimeError("boom — deliberate failure to drive the DLX path")

		br.register_consumer(
			queue_name="q",
			exchange_name="ex",
			exchange_type="direct",
			routing_key="rk",
			callback=failing_callback,
			auto_ack=False,
			dlx_enable=True,
			dlx_routing_key="custom.rk",
			enable_retry_cycles=True,
			max_retry_time_limit=0,
			retry_cycle_interval=1,
			use_quorum_queues=False,
		)
		br.publish(payload, exchange="ex", routing_key="rk")

	dlx_queue = f"q{config.DLX_SUFFIX}"
	parked = br.messages(dlx_queue)
	assert len(parked) == 1, f"No message arrived in {dlx_queue}"
	method = parked[0]
	assert method.body == payload, "DLX payload should match the original publish"
	# Regression guard for Issue 1b: the configured dlx_routing_key is what the
	# parked message is delivered with.
	assert method.routing_key == "custom.rk"
	assert method.headers.get("x-retry-exhausted") is True
	assert "x-processing-error" in method.headers
	assert method.headers.get("x-cycle-count") == 1


@pytest.mark.asyncio
async def test_async_terminal_dlx_on_exhaustion():
	async def failing(message, properties, body):
		raise RuntimeError("deliberate async failure")

	consumer = MrsalAsyncAMQP(**ASYNC_ARGS)
	async with TestMrsalAsyncBroker(consumer) as br:
		await br.register_consumer(
			queue_name="q",
			exchange_name="ex",
			exchange_type="direct",
			routing_key="rk",
			callback=failing,
			use_quorum_queues=False,
			dlx_enable=True,
			enable_retry_cycles=True,
			max_retry_time_limit=0,
			retry_cycle_interval=1,
		)
		await br.publish(b"async dlx", exchange="ex", routing_key="rk")

	dlx_queue = f"q{config.DLX_SUFFIX}"
	assert br.message_count("q") == 0
	assert br.message_count(dlx_queue) == 1
	parked = br.messages(dlx_queue)[0]
	assert parked.body == b"async dlx"
	assert parked.headers.get("x-retry-exhausted") is True
	assert parked.headers.get("x-cycle-count") == 1
