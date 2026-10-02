from unittest.mock import AsyncMock, MagicMock

from pydantic.dataclasses import dataclass

SETUP_ARGS = {
	'host': 'localhost',
	'port': 5672,
	'credentials': ('user', 'password'),
	'virtual_host': 'testboi',
	'prefetch_count': 1,
	'heartbeat': 60
}

@dataclass
class ExpectedPayload:
	id: int
	name: str
	active: bool


def mock_async_handles(consumer) -> None:
	"""Give ``consumer`` open mocked aio-pika connection and channel handles.

	Both carry ``close_callbacks``, which the consume loop registers on; so does
	any channel the connection opens (the DLX publish channel).
	"""
	consumer._connection = AsyncMock()
	consumer._connection.close_callbacks = MagicMock()
	consumer._connection.channel.return_value.close_callbacks = MagicMock()
	consumer._connection.is_closed = False
	consumer._channel = AsyncMock()
	consumer._channel.close_callbacks = MagicMock()
	consumer._channel.is_closed = False


class AsyncIteratorMock:
	"""Async-iterator + async-context-manager mock for aio_pika queue.iterator().

	After issue #74, start_consumer wraps the iterator in ``async with`` so the
	mock must satisfy both protocols. Shared across tests to avoid drift.
	"""
	def __init__(self, items):
		self.items = iter(list(items))

	def __aiter__(self):
		return self

	async def __anext__(self):
		try:
			return next(self.items)
		except StopIteration:
			raise StopAsyncIteration

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc, tb):
		return False

	async def close(self):
		pass


def make_queue_with_messages(messages):
	"""Build a mock queue whose iterator() returns an AsyncIteratorMock over ``messages``.

	Returns ``(mock_queue, fake_iterator)``. The ``fake_iterator`` exposes
	``iterator_call_kwargs`` so tests can assert the no_ack flag passed to
	``queue.iterator(...)``.
	"""
	fake_it = AsyncIteratorMock(messages)
	fake_it.iterator_call_kwargs = None
	mock_queue = AsyncMock()

	def iterator_factory(**kwargs):
		fake_it.iterator_call_kwargs = kwargs
		return fake_it

	mock_queue.iterator = MagicMock(side_effect=iterator_factory)
	return mock_queue, fake_it
