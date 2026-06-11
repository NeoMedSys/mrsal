"""Sync-side reconnect state recovery (issue #97).

Mirrors aio-pika's transparent restore on the blocking consumer: the declared
topology, QoS, and the connection they were declared on are tracked as instance
attributes so a reconnect's recovery is explicit and logged, not just an
accidental side effect of tenacity re-running ``start_consumer``. Recovery is
still *driven* by tenacity; broker topology is treated as durable
(auto_declare=False restores channel/QoS/consume, not re-declaration).
"""
import logging

import pytest
from unittest.mock import Mock, MagicMock, patch
from pika.exceptions import StreamLostError
from tenacity import wait_fixed
from mrsal.amqp.subclass import MrsalBlockingAMQP
from mrsal.testing import TestMrsalBroker

SETUP_ARGS = {
	'host': 'localhost',
	'port': 5672,
	'credentials': ('user', 'password'),
	'virtual_host': 'testboi',
	'ssl': False,
	'heartbeat': 60,
	'blocked_connection_timeout': 60,
	'prefetch_count': 7,
}


def _make_consumer(connection):
	"""A consumer wired to ``connection`` with topology declaration stubbed.

	``_setup_exchange_and_queue`` is replaced with a stub that just flips the
	success flag, so these tests exercise the tracking/reconnect logic without a
	broker. (The end-to-end declaration path is covered by the in-memory-broker
	test below.)
	"""
	consumer = MrsalBlockingAMQP(**SETUP_ARGS)
	consumer._connection = connection
	consumer._setup_exchange_and_queue = Mock(
		side_effect=lambda **_: setattr(consumer, 'auto_declare_ok', True)
	)
	return consumer


def _open_connection(channel=None):
	conn = MagicMock(name='connection')
	conn.is_open = True
	conn.channel.return_value = channel or MagicMock(name='channel')
	return conn


def _prepare(consumer, **overrides):
	kwargs = dict(
		queue_name='orders', callback=Mock(), callback_args=None,
		auto_ack=False, auto_declare=True, exchange_name='orders.x',
		exchange_type='direct', routing_key='orders.new', payload_model=None,
		dlx_enable=True, dlx_exchange_name=None, dlx_routing_key=None,
		use_quorum_queues=True, enable_retry_cycles=True, retry_cycle_interval=10,
		max_retry_time_limit=60, retry_backoff='exponential', retry_backoff_max=60,
		max_queue_length=None, max_queue_length_bytes=None, queue_overflow=None,
		single_active_consumer=None, lazy_queue=None, threaded=False,
	)
	kwargs.update(overrides)
	return consumer._prepare_consumer(**kwargs)


def test_prepare_records_full_topology_and_qos():
	"""A successful auto_declare=True setup records the full declaration + QoS.

	The record is the exact _setup_exchange_and_queue kwargs, so it is faithful
	enough to re-declare from (DLX/retry included via dlx_enable).
	"""
	conn = _open_connection()
	consumer = _make_consumer(conn)
	_prepare(consumer)

	assert consumer._active_qos == 7
	assert consumer._topology_conn is conn
	assert consumer._declared_topology == dict(
		exchange_name='orders.x', queue_name='orders', exchange_type='direct',
		routing_key='orders.new', dlx_enable=True, dlx_exchange_name=None,
		dlx_routing_key=None, use_quorum_queues=True, max_queue_length=None,
		max_queue_length_bytes=None, queue_overflow=None,
		single_active_consumer=None, lazy_queue=None, enable_retry_cycles=True,
		retry_cycle_interval=10, retry_backoff='exponential', retry_backoff_max=60,
	)


def test_auto_declare_false_records_qos_but_no_topology():
	"""auto_declare=False declares nothing, so only QoS is tracked.

	Broker topology is durable and survives the drop, so there is nothing for
	mrsal to re-declare -- acceptance criterion 2.
	"""
	conn = _open_connection()
	consumer = _make_consumer(conn)
	_prepare(
		consumer, auto_declare=False, exchange_name=None, exchange_type=None,
		routing_key=None, dlx_enable=False, enable_retry_cycles=False,
	)

	assert consumer._declared_topology is None
	assert consumer._active_qos == 7
	consumer._setup_exchange_and_queue.assert_not_called()


def test_reconnect_rebinds_tracking_to_new_connection():
	"""A new connection identity clears stale tracking and re-records against it."""
	conn1 = _open_connection()
	consumer = _make_consumer(conn1)
	_prepare(consumer)
	assert consumer._topology_conn is conn1

	# Simulate a reconnect: a fresh connection object replaces the old one.
	ch2 = MagicMock(name='channel2')
	conn2 = _open_connection(channel=ch2)
	consumer._connection = conn2
	_prepare(consumer)

	assert consumer._topology_conn is conn2
	assert consumer._consumer_channel is ch2
	assert consumer._declared_topology['queue_name'] == 'orders'


def test_prefetch_qos_reapplied_after_reconnect():
	"""prefetch_count set via basic_qos survives a reconnect -- acceptance criterion 3."""
	conn1 = _open_connection()
	consumer = _make_consumer(conn1)
	_prepare(consumer)

	ch2 = MagicMock(name='channel2')
	conn2 = _open_connection(channel=ch2)
	consumer._connection = conn2
	_prepare(consumer)

	ch2.basic_qos.assert_called_once_with(prefetch_count=7)
	assert consumer._active_qos == 7


def test_reconnect_is_logged_but_first_connect_is_not(caplog):
	"""The reconnect-recovery log fires only on a genuine reconnect, not first connect."""
	conn1 = _open_connection()
	consumer = _make_consumer(conn1)

	with caplog.at_level(logging.INFO, logger='mrsal.amqp.subclass'):
		_prepare(consumer)
		assert not [r for r in caplog.records if 'Reconnect recovery' in r.message]

		conn2 = _open_connection()
		consumer._connection = conn2
		caplog.clear()
		_prepare(consumer)

	recovery = [r for r in caplog.records if 'Reconnect recovery' in r.message]
	assert len(recovery) == 1
	assert 're-declared topology' in recovery[0].message
	assert "queue 'orders'" in recovery[0].message


def test_close_clears_tracking():
	conn = _open_connection()
	consumer = _make_consumer(conn)
	_prepare(consumer)
	assert consumer._declared_topology is not None and consumer._active_qos is not None

	consumer.close()

	assert consumer._declared_topology is None
	assert consumer._active_qos is None
	assert consumer._topology_conn is None


def test_inmemory_broker_records_real_topology():
	"""Higher-fidelity: tracking is populated by the REAL declaration path.

	Unlike the mock-connection tests above, this drives _prepare_consumer through
	the in-memory broker's real _setup_exchange_and_queue, and confirms _restore
	drops the tracking on context exit (no dangling ref to the fake connection).
	"""
	consumer = MrsalBlockingAMQP(**SETUP_ARGS)
	with TestMrsalBroker(consumer) as br:
		br.register_consumer(
			queue_name='orders',
			exchange_name='orders.x',
			exchange_type='direct',
			routing_key='orders.new',
			callback=lambda mf, p, b: None,
			dlx_enable=True,
			enable_retry_cycles=True,
		)
		assert consumer._active_qos == 7
		assert consumer._declared_topology is not None
		assert consumer._declared_topology['queue_name'] == 'orders'
		assert consumer._declared_topology['dlx_enable'] is True

	# Context exit clears tracking so nothing pins the discarded fake connection.
	assert consumer._declared_topology is None
	assert consumer._active_qos is None
	assert consumer._topology_conn is None


def test_consumer_resumes_against_same_queue_after_mid_consume_drop(monkeypatch):
	"""Kill the connection mid-consume; the consumer reconnects and resumes on the
	same queue without the caller re-running start_consumer -- acceptance criterion 1.

	Recovery is driven by tenacity re-running start_consumer; this verifies QoS
	is re-applied on the fresh connection and the message is processed after the
	drop.
	"""
	conn1 = _open_connection()
	ch1 = conn1.channel.return_value

	ch2 = MagicMock(name='channel2')
	conn2 = _open_connection(channel=ch2)

	frame = MagicMock()
	frame.delivery_tag = 1
	props = MagicMock()
	props.headers = None
	body = b'{"data": "ok"}'
	ch2.consume.return_value = [(frame, props, body)]

	def drop(*_a, **_k):
		# The broker reset the socket mid-consume.
		conn1.is_open = False
		raise StreamLostError()

	ch1.consume.side_effect = drop

	processed = []

	with patch('mrsal.amqp.subclass.MrsalBlockingAMQP.setup_blocking_connection',
			autospec=True) as mock_setup:
		def reconnect(self):
			self._connection = conn2
		mock_setup.side_effect = reconnect

		consumer = MrsalBlockingAMQP(**SETUP_ARGS)
		consumer._connection = conn1
		consumer._setup_exchange_and_queue = Mock(
			side_effect=lambda **_: setattr(consumer, 'auto_declare_ok', True)
		)

		# Keep tenacity's backoff out of the test wall-clock. start_consumer.retry
		# is a class-level object shared across instances; monkeypatch restores it
		# after the test even on failure. (Full xdist isolation would require
		# restructuring the @retry decorator and is out of scope.)
		monkeypatch.setattr(consumer.start_consumer.retry, 'wait', wait_fixed(0))

		consumer.start_consumer(
			queue_name='orders',
			exchange_name='orders.x',
			exchange_type='direct',
			routing_key='orders.new',
			callback=lambda mf, p, b: processed.append((mf.delivery_tag, b)),
			auto_ack=False,
			dlx_enable=False,
			enable_retry_cycles=False,
		)

	# The message delivered after the reconnect was processed and acked.
	assert processed == [(1, body)]
	ch2.basic_ack.assert_called_once_with(delivery_tag=1)
	# QoS was re-applied against the fresh connection's channel.
	ch2.basic_qos.assert_called_once_with(prefetch_count=7)
	# Tracking now points at the post-reconnect connection.
	assert consumer._consumer_channel is ch2
	assert consumer._topology_conn is conn2
	# The consumer resumed on the same queue.
	_, consume_kwargs = ch2.consume.call_args
	assert consume_kwargs['queue'] == 'orders'
