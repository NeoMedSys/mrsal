import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from mrsal.amqp.subclass import MrsalBlockingAMQP, MrsalAsyncAMQP


# --- Blocking close/context manager tests ---

@pytest.fixture
def mock_blocking():
	consumer = MrsalBlockingAMQP(
		host="localhost",
		port=5672,
		credentials=("user", "password"),
		virtual_host="testboi",
		ssl=False,
		prefetch_count=1,
		heartbeat=60
	)
	consumer._connection = MagicMock()
	consumer._connection.is_open = True
	consumer._consumer_channel = MagicMock()
	consumer._consumer_channel.is_open = True
	return consumer


def test_close_closes_consumer_channel_and_connection(mock_blocking):
	conn = mock_blocking._connection
	consumer_ch = mock_blocking._consumer_channel

	mock_blocking.close()

	consumer_ch.close.assert_called_once()
	conn.close.assert_called_once()
	assert mock_blocking._connection is None
	assert mock_blocking._consumer_channel is None


def test_close_is_safe_when_already_closed(mock_blocking):
	mock_blocking._connection = None
	mock_blocking._consumer_channel = None

	mock_blocking.close()  # should not raise


def test_close_skips_already_closed_consumer_channel(mock_blocking):
	consumer_ch = mock_blocking._consumer_channel
	consumer_ch.is_open = False
	conn = mock_blocking._connection

	mock_blocking.close()

	consumer_ch.close.assert_not_called()
	conn.close.assert_called_once()


def test_context_manager_calls_close(mock_blocking):
	with patch.object(mock_blocking, 'close') as mock_close:
		with mock_blocking:
			pass
		mock_close.assert_called_once()


def test_context_manager_calls_close_on_exception(mock_blocking):
	with patch.object(mock_blocking, 'close') as mock_close:
		with pytest.raises(RuntimeError):
			with mock_blocking:
				raise RuntimeError("boom")
		mock_close.assert_called_once()


# --- Async close/context manager tests ---

@pytest.fixture
def mock_async():
	consumer = MrsalAsyncAMQP(
		host="localhost",
		port=5672,
		credentials=("user", "password"),
		virtual_host="testboi",
		ssl=False,
		prefetch_count=1,
		heartbeat=60
	)
	consumer._connection = AsyncMock()
	consumer._connection.is_closed = False
	consumer._channel = AsyncMock()
	consumer._channel.is_closed = False
	return consumer


@pytest.mark.asyncio
async def test_async_close_closes_channel_and_connection(mock_async):
	conn = mock_async._connection
	ch = mock_async._channel

	await mock_async.close()

	ch.close.assert_awaited_once()
	conn.close.assert_awaited_once()
	assert mock_async._connection is None
	assert mock_async._channel is None


@pytest.mark.asyncio
async def test_async_close_is_safe_when_already_closed(mock_async):
	mock_async._connection = None
	mock_async._channel = None

	await mock_async.close()  # should not raise


@pytest.mark.asyncio
async def test_async_close_skips_already_closed_channel(mock_async):
	ch = mock_async._channel
	ch.is_closed = True

	await mock_async.close()

	ch.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_async_context_manager_calls_close(mock_async):
	with patch.object(mock_async, 'close', new_callable=AsyncMock) as mock_close:
		async with mock_async:
			pass
		mock_close.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_context_manager_calls_close_on_exception(mock_async):
	with patch.object(mock_async, 'close', new_callable=AsyncMock) as mock_close:
		with pytest.raises(RuntimeError):
			async with mock_async:
				raise RuntimeError("boom")
		mock_close.assert_awaited_once()
