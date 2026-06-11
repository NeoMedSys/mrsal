"""User-facing test harness: swap a mrsal consumer/publisher onto an
:class:`InMemoryBroker` and deliver messages to its handler inline.

``TestMrsalBroker`` (sync) and ``TestMrsalAsyncBroker`` (async) register a
consumer by running mrsal's real setup (``_prepare_consumer`` /
``_prepare_consumer_async`` — which declare the full topology, DLX included),
then deliver published messages straight into ``_process_single_message`` /
``_handle_message``. No background thread, no event loop juggling: a publish
runs the handler and returns, so assertions are deterministic.

Because delivery is inline and single-threaded, the harness does not model real
concurrency: ``threaded=True`` (sync) and ``max_concurrent_tasks`` (async) are
rejected rather than silently ignored.
"""
import json
import logging
from typing import Any

import pika

from mrsal import config
from mrsal.testing._broker import InMemoryBroker
from mrsal.testing._sync import (
	InMemoryBlockingConnection,
	build_basic_properties,
	build_method_frame,
	props_to_fields,
)
from mrsal.testing._async import InMemoryIncomingMessage, InMemoryRobustConnection

log = logging.getLogger(__name__)

# Defaults mirroring ``start_consumer`` so a caller only passes what they care
# about. ``_prepare_consumer`` is keyword-only with no defaults of its own.
_COMMON_DEFAULTS: dict[str, Any] = dict(
	callback=None,
	callback_args=None,
	auto_ack=False,
	auto_declare=True,
	exchange_name=None,
	exchange_type=None,
	routing_key=None,
	payload_model=None,
	dlx_enable=True,
	dlx_exchange_name=None,
	dlx_routing_key=None,
	use_quorum_queues=True,
	enable_retry_cycles=True,
	retry_cycle_interval=config.DEFAULT_RETRY_CYCLE_INTERVAL_MIN,
	max_retry_time_limit=config.DEFAULT_MAX_RETRY_TIME_LIMIT_MIN,
	retry_backoff=config.DEFAULT_RETRY_BACKOFF,
	retry_backoff_max=config.DEFAULT_RETRY_BACKOFF_MAX_MIN,
	max_queue_length=None,
	max_queue_length_bytes=None,
	queue_overflow=None,
	single_active_consumer=None,
	lazy_queue=None,
)

# ``start_consumer`` setup parameters the harness forwards to the prepare step.
_PREPARE_KEYS = frozenset(_COMMON_DEFAULTS) | {"queue_name"}
# Loop-timing parameters that are valid on ``start_consumer`` but no-ops for
# inline delivery; accepted and ignored so a pasted call site still works.
_SYNC_IGNORED = frozenset({"inactivity_timeout", "max_workers"})
_ASYNC_IGNORED = frozenset({"drain_timeout"})

# Safety bound so a pathological requeue loop can't hang a test.
_MAX_DELIVERIES_PER_PUMP = 10_000


def _encode(message: Any) -> bytes:
	if isinstance(message, bytes):
		return message
	if isinstance(message, str):
		return message.encode("utf-8")
	return json.dumps(message).encode("utf-8")


def _resolve_publish_target(exchange, routing_key, queue):
	"""Map publish kwargs to (exchange, routing_key).

	``queue=`` is sugar for the default exchange routing straight to that queue.
	"""
	if queue is not None:
		return "", queue
	return exchange or "", routing_key or ""


class _BaseTestBroker:
	"""Shared state and delivery plumbing for the sync/async harnesses."""

	__test__ = False  # not a pytest test class despite the ``Test*`` subclasses

	def __init__(self, consumer, broker: InMemoryBroker | None = None):
		self.consumer = consumer
		self.broker = broker if broker is not None else InMemoryBroker()
		self._registered: list[tuple[str, dict, bool]] = []
		self._orig_connection = None

	# connection swap / teardown --------------------------------------------

	def _swap_connection(self, connection) -> None:
		self._orig_connection = self.consumer._connection
		self.consumer._connection = connection

	def _restore(self) -> None:
		"""Restore the original connection and clear the now-dead fake channels."""
		self.consumer._connection = self._orig_connection
		for attr in ("_consumer_channel", "_channel", "_dlx_publish_channel"):
			if hasattr(self.consumer, attr):
				setattr(self.consumer, attr, None)
		# Drop declared-state tracking so it doesn't retain a reference to the
		# discarded in-memory connection (sync consumer only).
		if hasattr(self.consumer, "_reset_declared_state"):
			self.consumer._reset_declared_state()

	# registration helpers ---------------------------------------------------

	def _merge_prepare_kwargs(self, kwargs: dict, *, ignored: frozenset) -> dict:
		"""Validate caller kwargs and merge them over the consumer defaults."""
		if "queue_name" not in kwargs:
			raise TypeError("register_consumer() requires a queue_name")
		unknown = set(kwargs) - _PREPARE_KEYS - ignored
		if unknown:
			raise TypeError(
				f"register_consumer() got unexpected keyword argument(s): "
				f"{sorted(unknown)}. The harness accepts start_consumer's setup "
				"parameters; loop-timing/concurrency parameters are not modeled."
			)
		forwarded = {k: v for k, v in kwargs.items() if k in _PREPARE_KEYS}
		return {**_COMMON_DEFAULTS, **forwarded}

	def _enqueue_publish(self, message, *, exchange, routing_key, queue,
						properties, headers) -> None:
		exchange, routing_key = _resolve_publish_target(exchange, routing_key, queue)
		msg_headers = properties.headers if properties is not None else headers
		self.broker.publish(
			exchange=exchange,
			routing_key=routing_key,
			body=_encode(message),
			headers=msg_headers,
			props=props_to_fields(properties),
			mandatory=False,
		)

	def _drain_deliveries(self):
		"""Yield (runtime_config, delivery_tag, message) for each pending delivery."""
		for queue_name, runtime_config, auto_ack in self._registered:
			pending = self.broker.message_count(queue_name)
			if pending > _MAX_DELIVERIES_PER_PUMP:
				log.warning(
					"in-memory pump capped at %d deliveries for queue %r "
					"(%d pending); the remainder stays queued.",
					_MAX_DELIVERIES_PER_PUMP, queue_name, pending,
				)
			for _ in range(min(pending, _MAX_DELIVERIES_PER_PUMP)):
				got = self.broker.deliver_next(queue_name)
				if got is None:
					break
				delivery_tag, msg = got
				if auto_ack:
					self.broker.ack(delivery_tag)
				yield runtime_config, delivery_tag, msg

	# inspection -------------------------------------------------------------

	def message_count(self, queue_name: str) -> int:
		return self.broker.message_count(queue_name)

	def messages(self, queue_name: str):
		return self.broker.messages(queue_name)


class TestMrsalBroker(_BaseTestBroker):
	"""In-memory test harness for ``MrsalBlockingAMQP``."""

	def __enter__(self) -> "TestMrsalBroker":
		self._swap_connection(InMemoryBlockingConnection(self.broker))
		return self

	def __exit__(self, *_: Any) -> bool:
		self._restore()
		return False

	def register_consumer(self, **kwargs) -> None:
		"""Declare topology and register a handler, without entering the loop."""
		if kwargs.pop("threaded", False):
			raise ValueError(
				"threaded=True is not supported by the in-memory harness: it "
				"delivers handlers synchronously and cannot model a real "
				"ThreadPoolExecutor. Drop threaded=True for the test."
			)
		merged = self._merge_prepare_kwargs(kwargs, ignored=_SYNC_IGNORED)
		runtime_config = self.consumer._prepare_consumer(**merged, threaded=False)
		self._registered.append((merged["queue_name"], runtime_config, merged["auto_ack"]))

	def publish(self, message: Any, *, exchange: str | None = None,
				routing_key: str | None = None, queue: str | None = None,
				properties: pika.BasicProperties | None = None,
				headers: dict | None = None) -> None:
		"""Publish a message and run any registered handler it routes to."""
		self._enqueue_publish(
			message, exchange=exchange, routing_key=routing_key,
			queue=queue, properties=properties, headers=headers,
		)
		self._pump()

	def _pump(self) -> None:
		for runtime_config, delivery_tag, msg in self._drain_deliveries():
			self.consumer._process_single_message(
				build_method_frame(delivery_tag, msg),
				build_basic_properties(msg),
				msg.body,
				runtime_config,
			)

	def advance(self, **kwargs) -> None:
		"""Advance the modeled clock, then deliver anything a TTL expiry re-routed.

		Fires queue ``x-message-ttl`` expiry, which drives the ``.retry`` -> origin
		retry cycle. Note: retry-*budget* exhaustion is real-time in mrsal, so
		advancing re-cycles a message but never exhausts it -- force terminal DLX
		parking with a low ``max_retry_time_limit`` instead.
		"""
		self.broker.advance(**kwargs)
		self._pump()


class TestMrsalAsyncBroker(_BaseTestBroker):
	"""In-memory test harness for ``MrsalAsyncAMQP``."""

	async def __aenter__(self) -> "TestMrsalAsyncBroker":
		self._swap_connection(InMemoryRobustConnection(self.broker))
		return self

	async def __aexit__(self, *_: Any) -> bool:
		self._restore()
		return False

	async def register_consumer(self, **kwargs) -> None:
		if kwargs.pop("max_concurrent_tasks", None):
			raise ValueError(
				"max_concurrent_tasks is not supported by the in-memory harness: "
				"it delivers handlers sequentially and cannot model real asyncio "
				"concurrency. Drop max_concurrent_tasks for the test."
			)
		merged = self._merge_prepare_kwargs(kwargs, ignored=_ASYNC_IGNORED)
		_, runtime_config = await self.consumer._prepare_consumer_async(
			**merged, max_concurrent_tasks=None,
		)
		self._registered.append((merged["queue_name"], runtime_config, merged["auto_ack"]))

	async def publish(self, message: Any, *, exchange: str | None = None,
					routing_key: str | None = None, queue: str | None = None,
					properties: pika.BasicProperties | None = None,
					headers: dict | None = None) -> None:
		self._enqueue_publish(
			message, exchange=exchange, routing_key=routing_key,
			queue=queue, properties=properties, headers=headers,
		)
		await self._pump()

	async def _pump(self) -> None:
		for runtime_config, delivery_tag, msg in self._drain_deliveries():
			await self.consumer._handle_message(
				InMemoryIncomingMessage(self.broker, delivery_tag, msg),
				runtime_config,
			)

	async def advance(self, **kwargs) -> None:
		"""Advance the modeled clock, then deliver anything a TTL expiry re-routed.

		Fires queue ``x-message-ttl`` expiry, which drives the ``.retry`` -> origin
		retry cycle. Note: retry-*budget* exhaustion is real-time in mrsal, so
		advancing re-cycles a message but never exhausts it -- force terminal DLX
		parking with a low ``max_retry_time_limit`` instead.
		"""
		self.broker.advance(**kwargs)
		await self._pump()
