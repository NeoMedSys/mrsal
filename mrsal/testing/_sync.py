"""pika-shaped facade over :class:`InMemoryBroker`.

Implements just the surface ``MrsalBlockingAMQP`` / ``MrsalBlockingBase`` and
the superclass setup paths actually call on a ``BlockingConnection`` /
``BlockingChannel``, so mrsal's real code runs unmodified against the in-memory
broker.
"""
import pika
from pika.exceptions import UnroutableError
from typing import Any

from mrsal.testing._broker import (
	InMemoryBroker,
	PROPERTY_FIELDS,
	StoredMessage,
	UnroutableMessage,
)


def props_to_fields(properties: "pika.BasicProperties | None") -> dict[str, Any]:
	"""Flatten a ``pika.BasicProperties`` into the broker's ``props`` dict."""
	if properties is None:
		return {}
	return {f: getattr(properties, f, None) for f in PROPERTY_FIELDS}


def build_basic_properties(msg: StoredMessage) -> pika.BasicProperties:
	"""Rebuild ``pika.BasicProperties`` for delivery to a sync callback."""
	fields = {f: msg.props.get(f) for f in PROPERTY_FIELDS}
	return pika.BasicProperties(headers=msg.headers, **fields)


def build_method_frame(delivery_tag: int, msg: StoredMessage) -> "pika.spec.Basic.Deliver":
	return pika.spec.Basic.Deliver(
		consumer_tag="mrsal-inmemory",
		delivery_tag=delivery_tag,
		redelivered=msg.redelivered,
		exchange=msg.exchange,
		routing_key=msg.routing_key,
	)


class InMemoryBlockingChannel:
	"""Stand-in for ``pika.adapters.blocking_connection.BlockingChannel``."""

	def __init__(self, broker: InMemoryBroker):
		self._broker = broker
		self.is_open = True
		self._confirm_delivery = False

	# topology ---------------------------------------------------------------

	def basic_qos(self, prefetch_count: int = 0, **_: Any) -> None:
		return None

	def exchange_declare(self, exchange: str, exchange_type: str = "direct",
						arguments: dict | None = None, durable: bool = False,
						passive: bool = False, internal: bool = False,
						auto_delete: bool = False, **_: Any) -> None:
		self._broker.declare_exchange(
			exchange, exchange_type=exchange_type, durable=durable,
			arguments=arguments, passive=passive,
		)

	def queue_declare(self, queue: str, arguments: dict | None = None,
					durable: bool = False, exclusive: bool = False,
					auto_delete: bool = False, passive: bool = False, **_: Any) -> None:
		self._broker.declare_queue(queue, arguments=arguments, durable=durable, passive=passive)

	def queue_bind(self, queue: str, exchange: str, routing_key: str | None = None,
				arguments: dict | None = None, **_: Any) -> None:
		self._broker.bind_queue(exchange=exchange, queue=queue, routing_key=routing_key, arguments=arguments)

	# publish ----------------------------------------------------------------

	def confirm_delivery(self) -> None:
		self._confirm_delivery = True

	def basic_publish(self, exchange: str, routing_key: str, body,
					properties: "pika.BasicProperties | None" = None,
					mandatory: bool = False, **_: Any) -> None:
		if isinstance(body, str):
			body = body.encode("utf-8")
		try:
			self._broker.publish(
				exchange=exchange,
				routing_key=routing_key,
				body=body,
				headers=getattr(properties, "headers", None),
				props=props_to_fields(properties),
				mandatory=mandatory,
			)
		except UnroutableMessage as exc:
			# Under publisher confirms a returned mandatory publish surfaces as
			# UnroutableError; mrsal's DLX publish relies on exactly this.
			raise UnroutableError(messages=[(exchange, routing_key, body)]) from exc

	# ack --------------------------------------------------------------------

	def basic_ack(self, delivery_tag: int = 0, multiple: bool = False, **_: Any) -> None:
		self._broker.ack(delivery_tag)

	def basic_nack(self, delivery_tag: int = 0, multiple: bool = False,
				requeue: bool = True, **_: Any) -> None:
		self._broker.nack(delivery_tag, requeue=requeue)

	def consume(self, *_: Any, **__: Any):
		"""The in-memory harness delivers inline; it never drives the blocking loop."""
		raise NotImplementedError(
			"The in-memory broker delivers messages inline. Use "
			"TestMrsalBroker.register_consumer(...) + publish(...) instead of "
			"start_consumer(...) when wrapping a consumer for tests."
		)

	def close(self) -> None:
		self.is_open = False


class InMemoryBlockingConnection:
	"""Stand-in for ``pika.BlockingConnection``."""

	def __init__(self, broker: InMemoryBroker):
		self._broker = broker
		self.is_open = True

	def channel(self) -> InMemoryBlockingChannel:
		return InMemoryBlockingChannel(self._broker)

	def add_callback_threadsafe(self, callback) -> None:
		# Single-threaded broker: run inline (matches the non-threaded path).
		callback()

	def close(self) -> None:
		self.is_open = False
