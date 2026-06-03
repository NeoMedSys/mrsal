import queue
import pytest
from unittest.mock import patch, MagicMock

from pika.exceptions import UnroutableError, NackError, StreamLostError

from mrsal.amqp.subclass import (
	MrsalBlockingPublisher,
	MrsalBlockingPublisherPool,
	_PUBLISH_ATTEMPTS,
)
from mrsal.exceptions import MrsalAbortedSetup


SETUP_ARGS = {
	'host': 'localhost',
	'port': 5672,
	'credentials': ('user', 'password'),
	'virtual_host': 'testboi',
	'ssl': False,
	'heartbeat': 60,
	'blocked_connection_timeout': 60,
	'prefetch_count': 1,
}

PUBLISH_ARGS = {
	'exchange_name': 'testExch',
	'routing_key': 'test-route',
	'message': 'hello',
	'exchange_type': 'direct',
	'queue_name': 'testQueue',
}


# --- Publisher fixtures -----------------------------------------------------

@pytest.fixture
def mock_conn():
	"""Patch the blocking connection so setup_blocking_connection wires a mock.

	Mirrors the boundary mocking used by the other blocking unit tests: pika's
	BlockingConnection never actually connects, and the consumer/publisher gets
	a MagicMock channel injected.
	"""
	with patch('mrsal.amqp.subclass.pika.BlockingConnection') as mock_bc, \
		patch('mrsal.amqp.subclass.MrsalBlockingPublisher.setup_blocking_connection', autospec=True) as mock_setup:

		mock_channel = MagicMock()
		mock_channel.is_open = True
		mock_connection = MagicMock()
		mock_connection.is_open = True
		mock_connection.channel.return_value = mock_channel
		mock_bc.return_value = mock_connection

		def _setup(self):
			self._connection = mock_connection
		mock_setup.side_effect = _setup

		yield mock_connection, mock_channel


@pytest.fixture
def publisher(mock_conn):
	pub = MrsalBlockingPublisher(**SETUP_ARGS)
	# _setup_exchange_and_queue talks to the broker; stub it and assert call counts.
	# A real successful declare sets auto_declare_ok, which publish() guards on, so
	# the stub mirrors that.
	def _declare_ok(*args, **kwargs):
		pub.auto_declare_ok = True
	pub._setup_exchange_and_queue = MagicMock(side_effect=_declare_ok)
	return pub


# --- Topology cache ---------------------------------------------------------

def test_declare_runs_once_then_is_cached(publisher, mock_conn):
	"""First publish to a target declares; subsequent ones skip the declare."""
	publisher.publish(**PUBLISH_ARGS)
	publisher.publish(**PUBLISH_ARGS)
	publisher.publish(**PUBLISH_ARGS)

	assert publisher._setup_exchange_and_queue.call_count == 1
	_, mock_channel = mock_conn
	assert mock_channel.basic_publish.call_count == 3


def test_distinct_targets_each_declare_once(publisher):
	publisher.publish(**PUBLISH_ARGS)
	publisher.publish(**{**PUBLISH_ARGS, 'exchange_name': 'otherExch', 'queue_name': 'otherQueue'})

	assert publisher._setup_exchange_and_queue.call_count == 2
	assert len(publisher._declared_topology) == 2


def test_auto_declare_false_never_declares(publisher, mock_conn):
	publisher.publish(**PUBLISH_ARGS, auto_declare=False)

	publisher._setup_exchange_and_queue.assert_not_called()
	_, mock_channel = mock_conn
	assert mock_channel.basic_publish.call_count == 1


# --- Channel reuse ----------------------------------------------------------

def test_channel_is_reused_across_publishes(publisher, mock_conn):
	mock_connection, mock_channel = mock_conn

	publisher.publish(**PUBLISH_ARGS)
	publisher.publish(**{**PUBLISH_ARGS, 'exchange_name': 'otherExch', 'queue_name': 'otherQueue'})

	# One channel opened, confirms enabled once, never closed between publishes.
	assert mock_connection.channel.call_count == 1
	mock_channel.confirm_delivery.assert_called_once()
	mock_channel.close.assert_not_called()


# --- Reconnect invalidates the cache ----------------------------------------

def test_reconnect_clears_topology_cache(publisher):
	publisher.publish(**PUBLISH_ARGS)
	assert publisher._setup_exchange_and_queue.call_count == 1

	# Simulate the connection being swapped out by a reconnect.
	new_conn = MagicMock()
	new_conn.is_open = True
	new_conn.channel.return_value = MagicMock(is_open=True)
	publisher._connection = new_conn

	publisher.publish(**PUBLISH_ARGS)
	# New socket has verified nothing, so the declare runs again.
	assert publisher._setup_exchange_and_queue.call_count == 2


# --- Message validation -----------------------------------------------------

def test_publish_rejects_non_str_bytes(publisher):
	with pytest.raises(MrsalAbortedSetup):
		publisher.publish(**{**PUBLISH_ARGS, 'message': {'not': 'allowed'}})


# --- Error handling ---------------------------------------------------------

def test_unroutable_is_raised_not_retried(publisher, mock_conn):
	_, mock_channel = mock_conn
	mock_channel.basic_publish.side_effect = UnroutableError('unroutable')

	with pytest.raises(UnroutableError):
		publisher.publish(**PUBLISH_ARGS)
	# Terminal: attempted exactly once, no reconnect loop.
	assert mock_channel.basic_publish.call_count == 1


def test_nack_is_raised_not_retried(publisher, mock_conn):
	_, mock_channel = mock_conn
	mock_channel.basic_publish.side_effect = NackError([])

	with pytest.raises(NackError):
		publisher.publish(**PUBLISH_ARGS)
	assert mock_channel.basic_publish.call_count == 1


def test_connection_error_retries_then_raises(publisher, mock_conn, monkeypatch):
	_, mock_channel = mock_conn
	mock_channel.basic_publish.side_effect = StreamLostError('dropped')
	monkeypatch.setattr('mrsal.amqp.subclass.time.sleep', lambda *a, **k: None)

	with pytest.raises(StreamLostError):
		publisher.publish(**PUBLISH_ARGS)
	# Retried up to the attempt cap.
	assert mock_channel.basic_publish.call_count == _PUBLISH_ATTEMPTS


def test_publish_sets_mandatory_true(publisher, mock_conn):
	_, mock_channel = mock_conn
	publisher.publish(**PUBLISH_ARGS)
	# mandatory=True is what makes an unroutable target raise instead of vanish.
	assert mock_channel.basic_publish.call_args.kwargs.get('mandatory') is True


def test_declare_failure_raises_and_is_not_cached(publisher, mock_conn):
	# _setup_exchange_and_queue swallows the broker error and leaves
	# auto_declare_ok False -- publish() must surface that, not publish blindly.
	def _declare_fail(*args, **kwargs):
		publisher.auto_declare_ok = False
	publisher._setup_exchange_and_queue = MagicMock(side_effect=_declare_fail)

	with pytest.raises(MrsalAbortedSetup):
		publisher.publish(**PUBLISH_ARGS)

	# Failed target not cached, declared once (no wasted retry loop), nothing sent.
	assert publisher._declared_topology == set()
	assert publisher._setup_exchange_and_queue.call_count == 1
	_, mock_channel = mock_conn
	mock_channel.basic_publish.assert_not_called()


def test_close_clears_channel_and_cache(publisher, mock_conn):
	publisher.publish(**PUBLISH_ARGS)
	assert publisher._publish_channel is not None
	assert publisher._declared_topology

	publisher.close()

	assert publisher._publish_channel is None
	assert publisher._declared_topology == set()
	assert publisher._topology_conn is None


# --- Pool -------------------------------------------------------------------

@pytest.fixture
def mock_publisher_cls():
	"""Replace MrsalBlockingPublisher with a factory of MagicMock instances.

	Isolates pool mechanics (checkout/checkin/grow/bound/close) from the real
	connection logic, which is covered by the publisher tests above.
	"""
	with patch('mrsal.amqp.subclass.MrsalBlockingPublisher') as cls:
		instances = []

		def factory(**kwargs):
			inst = MagicMock(name=f'publisher-{len(instances)}')
			instances.append(inst)
			return inst

		cls.side_effect = factory
		cls.instances = instances
		yield cls


def _make_pool(size):
	return MrsalBlockingPublisherPool(
		size=size, host='localhost', port=5672,
		credentials=('user', 'password'), virtual_host='testboi',
	)


def test_pool_size_must_be_positive():
	with pytest.raises(ValueError):
		_make_pool(size=0)


def test_pool_reuses_warm_publisher(mock_publisher_cls):
	pool = _make_pool(size=2)

	with pool.acquire() as p1:
		pass
	with pool.acquire() as p2:
		pass

	# The returned publisher is reused rather than a second one created.
	assert len(mock_publisher_cls.instances) == 1
	assert p1 is p2


def test_pool_grows_to_size_under_concurrency(mock_publisher_cls):
	pool = _make_pool(size=2)

	with pool.acquire() as p1:
		with pool.acquire() as p2:
			assert p1 is not p2
			assert len(mock_publisher_cls.instances) == 2


def test_pool_saturation_raises_on_timeout(mock_publisher_cls):
	pool = _make_pool(size=1)

	with pool.acquire():
		with pytest.raises(queue.Empty):
			with pool.acquire(timeout=0.05):
				pass


def test_close_all_closes_publishers_and_blocks_checkout(mock_publisher_cls):
	pool = _make_pool(size=2)
	with pool.acquire():
		pass

	pool.close_all()

	for inst in mock_publisher_cls.instances:
		inst.close.assert_called_once()
	with pytest.raises(RuntimeError):
		with pool.acquire():
			pass


def test_checkin_on_closed_pool_closes_publisher(mock_publisher_cls):
	pool = _make_pool(size=1)
	pub = pool._checkout(timeout=None)
	pool._closed = True

	pool._checkin(pub)

	pub.close.assert_called_once()
