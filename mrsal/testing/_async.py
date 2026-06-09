"""aio-pika-shaped facade over :class:`InMemoryBroker`.

Implements the subset of the aio-pika connection / channel / queue / exchange /
message surface that ``MrsalAsyncAMQP`` and the superclass async setup paths
use, so the real async code runs unmodified against the in-memory broker.
"""
from typing import Any

from mrsal.testing._broker import InMemoryBroker, PROPERTY_FIELDS, StoredMessage


class InMemoryIncomingMessage:
	"""Stand-in for ``aio_pika.IncomingMessage`` delivered to an async callback."""

	def __init__(self, broker: InMemoryBroker, delivery_tag: int, msg: StoredMessage):
		self._broker = broker
		self.delivery_tag = delivery_tag
		self.body = msg.body
		self.exchange = msg.exchange
		self.routing_key = msg.routing_key
		self.redelivered = msg.redelivered
		self.headers = msg.headers or {}
		for fname in PROPERTY_FIELDS:
			setattr(self, fname, msg.props.get(fname))

	async def ack(self, multiple: bool = False) -> None:
		self._broker.ack(self.delivery_tag)

	async def reject(self, requeue: bool = False) -> None:
		self._broker.nack(self.delivery_tag, requeue=requeue)

	async def nack(self, multiple: bool = False, requeue: bool = False) -> None:
		self._broker.nack(self.delivery_tag, requeue=requeue)


def _message_to_props(message: Any) -> dict[str, Any]:
	return {f: getattr(message, f, None) for f in PROPERTY_FIELDS}


class InMemoryAsyncExchange:
	def __init__(self, broker: InMemoryBroker, name: str):
		self._broker = broker
		self.name = name

	async def publish(self, message: Any, routing_key: str,
					mandatory: bool = True, **_: Any) -> None:
		body = message.body
		if isinstance(body, str):
			body = body.encode("utf-8")
		self._broker.publish(
			exchange=self.name,
			routing_key=routing_key,
			body=body,
			headers=getattr(message, "headers", None),
			props=_message_to_props(message),
			mandatory=mandatory,
		)


class InMemoryAsyncQueue:
	def __init__(self, broker: InMemoryBroker, name: str):
		self._broker = broker
		self.name = name

	async def bind(self, exchange: Any, routing_key: str | None = None,
				arguments: dict | None = None, **_: Any) -> None:
		exchange_name = exchange.name if hasattr(exchange, "name") else str(exchange)
		self._broker.bind_queue(
			exchange=exchange_name, queue=self.name,
			routing_key=routing_key, arguments=arguments,
		)

	def iterator(self, *_: Any, **__: Any):
		"""The in-memory harness delivers inline; it never drives the blocking loop."""
		raise NotImplementedError(
			"The in-memory broker delivers messages inline. Use "
			"TestMrsalAsyncBroker.register_consumer(...) + publish(...) instead of "
			"start_consumer(...) when wrapping a consumer for tests."
		)


class InMemoryAsyncChannel:
	def __init__(self, broker: InMemoryBroker):
		self._broker = broker
		self.is_closed = False

	async def set_qos(self, prefetch_count: int = 0, **_: Any) -> None:
		return None

	async def declare_exchange(self, name: str, type: str = "direct",
							durable: bool = True, auto_delete: bool = False,
							internal: bool = False, arguments: dict | None = None,
							passive: bool = False, **_: Any) -> InMemoryAsyncExchange:
		ex_type = type.value if hasattr(type, "value") else str(type)
		self._broker.declare_exchange(
			name, exchange_type=ex_type, durable=durable,
			arguments=arguments, passive=passive,
		)
		return InMemoryAsyncExchange(self._broker, name)

	async def declare_queue(self, name: str, durable: bool = True,
						exclusive: bool = False, auto_delete: bool = False,
						arguments: dict | None = None, passive: bool = False,
						**_: Any) -> InMemoryAsyncQueue:
		self._broker.declare_queue(name, arguments=arguments, durable=durable, passive=passive)
		return InMemoryAsyncQueue(self._broker, name)

	async def get_exchange(self, name: str, **_: Any) -> InMemoryAsyncExchange:
		return InMemoryAsyncExchange(self._broker, name)

	async def close(self) -> None:
		self.is_closed = True


class InMemoryRobustConnection:
	"""Stand-in for the object returned by ``aio_pika.connect_robust``."""

	def __init__(self, broker: InMemoryBroker):
		self._broker = broker
		self.is_closed = False

	async def channel(self, publisher_confirms: bool = True, **_: Any) -> InMemoryAsyncChannel:
		return InMemoryAsyncChannel(self._broker)

	async def close(self) -> None:
		self.is_closed = True
