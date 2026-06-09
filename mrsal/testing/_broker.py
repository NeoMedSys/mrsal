"""Transport-agnostic in-memory AMQP broker core.

Models exchanges, queues and bindings as plain Python so mrsal's real
consumer/publisher code paths can run against it without a live RabbitMQ.
Routing (direct / topic / fanout / headers / default exchange) and the
unacked-delivery bookkeeping live here; the pika-shaped and aio-pika-shaped
facades in ``_sync`` / ``_async`` translate library calls into these methods.

Time is modeled, not real: ``now_ms`` only advances when a test calls
``advance(...)``, which is what fires queue ``x-message-ttl`` expiry and drives
the ``.retry`` -> origin-queue retry cycle deterministically. Note that mrsal's
retry-*budget* exhaustion (``x-total-elapsed`` vs ``max_retry_time_limit``) is
computed from real wall-clock time in the superclass, so ``advance()`` re-cycles
a message but does not exhaust it -- force terminal DLX parking with a low
``max_retry_time_limit`` instead.
"""
from collections import deque
from dataclasses import dataclass, field
from typing import Any

# The pika.BasicProperties / aio_pika.Message field surface the broker carries
# through alongside headers/body. Single source for both facades so the sync and
# async property mapping can't drift.
PROPERTY_FIELDS = (
	"message_id", "app_id", "content_type", "content_encoding", "delivery_mode",
	"correlation_id", "reply_to", "priority", "timestamp", "type", "user_id",
	"expiration", "cluster_id",
)


@dataclass
class StoredMessage:
	"""One message at rest in a queue (or in flight as an unacked delivery)."""
	body: bytes
	exchange: str
	routing_key: str
	headers: dict[str, Any] | None = None
	# Remaining pika.BasicProperties fields, kept as a flat dict so each
	# transport can rebuild its own properties object on delivery.
	props: dict[str, Any] = field(default_factory=dict)
	redelivered: bool = False
	# Modeled-clock bookkeeping for TTL / dead-lettering (set on enqueue).
	enqueued_at_ms: int = 0
	expiration_ms: int | None = None


def parse_expiration(value: Any) -> int | None:
	"""Normalize an AMQP ``expiration`` to milliseconds.

	pika carries it as a string of ms; aio-pika as a ``timedelta``; either may
	already be a number.
	"""
	if value is None:
		return None
	if isinstance(value, bool):
		return None
	if isinstance(value, (int, float)):
		return int(value)
	if isinstance(value, str):
		try:
			return int(value)
		except ValueError:
			return None
	total_seconds = getattr(value, "total_seconds", None)
	if callable(total_seconds):
		return int(total_seconds() * 1000)
	return None


@dataclass
class _Queue:
	name: str
	arguments: dict[str, Any] | None
	messages: deque = field(default_factory=deque)


def topic_matches(pattern: str, routing_key: str) -> bool:
	"""AMQP topic match: ``*`` matches exactly one word, ``#`` zero or more."""
	p = pattern.split(".")
	k = routing_key.split(".")

	# Classic DP over (pattern_index, key_index).
	def match(pi: int, ki: int) -> bool:
		if pi == len(p):
			return ki == len(k)
		if p[pi] == "#":
			# '#' consumes zero or more words.
			for skip in range(0, len(k) - ki + 1):
				if match(pi + 1, ki + skip):
					return True
			return False
		if ki == len(k):
			return False
		if p[pi] == "*" or p[pi] == k[ki]:
			return match(pi + 1, ki + 1)
		return False

	return match(0, 0)


class UnroutableMessage(Exception):
	"""Raised by the broker when a mandatory publish matches no queue.

	The sync / async facades translate this into the library-native error
	(``pika.exceptions.UnroutableError`` / ``aio_pika`` delivery error).
	"""

	def __init__(self, exchange: str, routing_key: str):
		super().__init__(f"no queue bound for exchange={exchange!r} routing_key={routing_key!r}")
		self.exchange = exchange
		self.routing_key = routing_key


class InMemoryBroker:
	"""Single-node, in-process AMQP broker used by the mrsal test harness."""

	def __init__(self) -> None:
		# exchange name -> {"type": str, "durable": bool, "arguments": dict|None}
		self.exchanges: dict[str, dict[str, Any]] = {}
		# (exchange, routing_key, queue, arguments)
		self.bindings: list[tuple[str, str, str, dict[str, Any] | None]] = []
		self.queues: dict[str, _Queue] = {}
		# delivery_tag -> (queue_name, StoredMessage)
		self._unacked: dict[int, tuple[str, StoredMessage]] = {}
		self._delivery_seq: int = 0
		self.now_ms: int = 0

		# The default (nameless) direct exchange always exists; a publish to ""
		# routes to the queue whose name equals the routing key.
		self.exchanges[""] = {"type": "direct", "durable": True, "arguments": None}

	# -- clock ---------------------------------------------------------------

	def advance(self, *, minutes: float = 0, seconds: float = 0, ms: float = 0) -> None:
		"""Advance the modeled clock, expiring any now-due TTL messages.

		Expired messages are dead-lettered per their queue's
		``x-dead-letter-exchange`` / ``x-dead-letter-routing-key`` arguments --
		this is what drives the ``.retry`` -> origin-queue cycle.
		"""
		self.now_ms += int(minutes * 60_000 + seconds * 1000 + ms)
		self._expire_due()

	# -- topology ------------------------------------------------------------

	def declare_exchange(self, name: str, *, exchange_type: str = "direct",
						durable: bool = True, arguments: dict[str, Any] | None = None,
						passive: bool = False) -> None:
		if passive:
			if name not in self.exchanges:
				raise KeyError(f"exchange {name!r} does not exist")
			return
		# Idempotent re-declare keeps the original definition (mrsal re-declares
		# the same topology on reconnect / repeated setup).
		self.exchanges.setdefault(name, {"type": exchange_type, "durable": durable, "arguments": arguments})

	def declare_queue(self, name: str, *, arguments: dict[str, Any] | None = None,
					durable: bool = True, passive: bool = False) -> str:
		if passive:
			if name not in self.queues:
				raise KeyError(f"queue {name!r} does not exist")
			return name
		self.queues.setdefault(name, _Queue(name=name, arguments=arguments))
		return name

	def bind_queue(self, *, exchange: str, queue: str, routing_key: str | None,
				arguments: dict[str, Any] | None = None) -> None:
		key = routing_key or ""
		binding = (exchange, key, queue, arguments)
		if binding not in self.bindings:
			self.bindings.append(binding)

	# -- routing -------------------------------------------------------------

	def route(self, exchange: str, routing_key: str,
			headers: dict[str, Any] | None = None) -> list[str]:
		"""Return the queue names a publish should land in."""
		if exchange == "":
			# Default exchange: direct-to-queue-by-name.
			return [routing_key] if routing_key in self.queues else []

		ex = self.exchanges.get(exchange)
		if ex is None:
			return []
		ex_type = ex["type"]

		matched: list[str] = []
		for b_exchange, b_key, b_queue, b_args in self.bindings:
			if b_exchange != exchange:
				continue
			if b_queue not in self.queues:
				continue
			if ex_type == "fanout":
				ok = True
			elif ex_type == "topic":
				ok = topic_matches(b_key, routing_key)
			elif ex_type == "headers":
				ok = self._headers_match(b_args, headers)
			else:  # direct
				ok = b_key == routing_key
			if ok and b_queue not in matched:
				matched.append(b_queue)
		return matched

	@staticmethod
	def _headers_match(bind_args: dict[str, Any] | None,
					msg_headers: dict[str, Any] | None) -> bool:
		bind_args = bind_args or {}
		msg_headers = msg_headers or {}
		match_mode = bind_args.get("x-match", "all")
		expected = {k: v for k, v in bind_args.items() if k != "x-match"}
		if not expected:
			return False
		results = [msg_headers.get(k) == v for k, v in expected.items()]
		return all(results) if match_mode == "all" else any(results)

	# -- publish / deliver / ack --------------------------------------------

	def publish(self, *, exchange: str, routing_key: str, body: bytes,
				headers: dict[str, Any] | None = None,
				props: dict[str, Any] | None = None,
				mandatory: bool = False) -> list[str]:
		"""Route and enqueue a message. Returns the queues it landed in.

		Raises ``UnroutableMessage`` when ``mandatory`` is set and nothing
		matched — mirroring RabbitMQ's basic.return under publisher confirms.
		"""
		targets = self.route(exchange, routing_key, headers)
		if not targets and mandatory:
			raise UnroutableMessage(exchange, routing_key)
		props = dict(props) if props else {}
		for queue_name in targets:
			self.queues[queue_name].messages.append(
				StoredMessage(
					body=body,
					exchange=exchange,
					routing_key=routing_key,
					headers=dict(headers) if headers else None,
					props=dict(props),
					enqueued_at_ms=self.now_ms,
					expiration_ms=parse_expiration(props.get("expiration")),
				)
			)
		return targets

	def deliver_next(self, queue_name: str) -> tuple[int, StoredMessage] | None:
		"""Pop the head message of a queue and register it as an unacked delivery."""
		q = self.queues.get(queue_name)
		if q is None or not q.messages:
			return None
		msg = q.messages.popleft()
		self._delivery_seq += 1
		tag = self._delivery_seq
		self._unacked[tag] = (queue_name, msg)
		return tag, msg

	def ack(self, delivery_tag: int) -> None:
		self._unacked.pop(delivery_tag, None)

	def nack(self, delivery_tag: int, *, requeue: bool) -> None:
		entry = self._unacked.pop(delivery_tag, None)
		if entry is None:
			return
		queue_name, msg = entry
		if requeue:
			msg.redelivered = True
			self.queues[queue_name].messages.appendleft(msg)
			return
		# requeue=False: dead-letter via the queue's x-dead-letter-* args if set
		# (this is the dlx_enable-without-retry-cycles path), else drop.
		q = self.queues.get(queue_name)
		if q is not None:
			self._dead_letter(q, msg)

	# -- dead-lettering / TTL expiry ----------------------------------------

	def _dead_letter(self, queue: _Queue, msg: StoredMessage) -> None:
		"""Re-route a rejected / expired message to its queue's DLX, or drop it."""
		args = queue.arguments or {}
		dl_exchange = args.get("x-dead-letter-exchange")
		if dl_exchange is None:
			return  # no DLX configured -> message is dropped
		dl_routing_key = args.get("x-dead-letter-routing-key", msg.routing_key)
		self.publish(
			exchange=dl_exchange,
			routing_key=dl_routing_key,
			body=msg.body,
			headers=msg.headers,
			props=msg.props,
			mandatory=False,
		)

	def _deadline_ms(self, queue: _Queue, msg: StoredMessage) -> int | None:
		"""When this message expires, honoring the shorter of queue/message TTL."""
		ttls: list[int] = []
		queue_ttl = (queue.arguments or {}).get("x-message-ttl")
		if queue_ttl is not None:
			ttls.append(int(queue_ttl))
		if msg.expiration_ms is not None:
			ttls.append(msg.expiration_ms)
		if not ttls:
			return None
		return msg.enqueued_at_ms + min(ttls)

	def _expire_due(self) -> None:
		"""Dead-letter every message whose TTL has elapsed at the current clock."""
		for _ in range(100):  # bounded: chained TTL hops settle in a few passes
			expired_any = False
			for queue in list(self.queues.values()):
				kept = deque()
				due: list[StoredMessage] = []
				for msg in queue.messages:
					deadline = self._deadline_ms(queue, msg)
					if deadline is not None and self.now_ms >= deadline:
						due.append(msg)
					else:
						kept.append(msg)
				if due:
					queue.messages = kept
					for msg in due:
						self._dead_letter(queue, msg)
						expired_any = True
			if not expired_any:
				return

	# -- inspection (assertions) --------------------------------------------

	def message_count(self, queue_name: str) -> int:
		q = self.queues.get(queue_name)
		return len(q.messages) if q is not None else 0

	def messages(self, queue_name: str) -> list[StoredMessage]:
		q = self.queues.get(queue_name)
		return list(q.messages) if q is not None else []
