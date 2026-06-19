"""Gap 2: structured consume/publish log fields + logger injection.

Exercised against the in-memory broker (Issue 4) so the records are emitted by
the real consume / DLX machinery. A capturing ``logging.Handler`` on an injected
logger proves both facets at once: that ``set_logger`` reroutes mrsal's records,
and that the consume-lifecycle records carry the structured ``extra`` fields.
"""
import logging

from mrsal.amqp.subclass import MrsalBlockingAMQP
from mrsal.testing import TestMrsalBroker
from pydantic import BaseModel


SYNC_ARGS = {
	"host": "localhost",
	"port": 5672,
	"credentials": ("guest", "guest"),
	"virtual_host": "/",
	"ssl": False,
	"prefetch_count": 1,
}

CONSUME_FIELDS = {"msg_id", "app_id", "queue", "routing_key", "retry", "outcome", "duration_ms"}


class CapturingHandler(logging.Handler):
	"""Collects emitted ``LogRecord`` objects for assertion."""

	def __init__(self):
		super().__init__()
		self.records: list[logging.LogRecord] = []

	def emit(self, record):
		self.records.append(record)

	def with_outcome(self, outcome):
		return [r for r in self.records if getattr(r, "outcome", None) == outcome]


def _make_logger(name):
	logger = logging.getLogger(name)
	logger.handlers.clear()
	logger.setLevel(logging.DEBUG)
	handler = CapturingHandler()
	logger.addHandler(handler)
	logger.propagate = False
	return logger, handler


def _register_sync(br, callback, **overrides):
	kwargs = dict(
		queue_name="q",
		exchange_name="ex",
		exchange_type="direct",
		routing_key="rk",
		callback=callback,
		use_quorum_queues=False,
		dlx_enable=False,
		enable_retry_cycles=False,
	)
	kwargs.update(overrides)
	br.register_consumer(**kwargs)


def test_injected_logger_receives_consume_record_with_fields():
	logger, handler = _make_logger("test.mrsal.injected")
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_logger(logger)

	with TestMrsalBroker(consumer) as br:
		_register_sync(br, lambda mf, props, body: None)
		br.publish(b"ok", exchange="ex", routing_key="rk")

	processed = handler.with_outcome("processed")
	assert len(processed) == 1
	rec = processed[0]
	# All structured fields present and discrete on the record.
	assert CONSUME_FIELDS <= set(vars(rec))
	assert rec.queue == "q"
	assert rec.routing_key == "rk"
	assert isinstance(rec.duration_ms, float)


def test_validation_failure_outcome_field():
	logger, handler = _make_logger("test.mrsal.validation")
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_logger(logger)

	class Schema(BaseModel):
		order_id: str

	with TestMrsalBroker(consumer) as br:
		_register_sync(br, lambda mf, props, body: None, payload_model=Schema)
		br.publish(b"not json", exchange="ex", routing_key="rk")

	assert len(handler.with_outcome("validation_failed")) == 1


def test_callback_failure_outcome_field():
	logger, handler = _make_logger("test.mrsal.callback")
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_logger(logger)

	def boom(mf, props, body):
		raise RuntimeError("nope")

	with TestMrsalBroker(consumer) as br:
		_register_sync(br, boom)
		br.publish(b"bad", exchange="ex", routing_key="rk")

	assert len(handler.with_outcome("callback_failed")) == 1


def test_no_dlx_drop_carries_outcome_dropped():
	# Callback fails with no DLX declared -> message is dropped, logged outcome='dropped'.
	logger, handler = _make_logger("test.mrsal.dropped")
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_logger(logger)

	def boom(mf, props, body):
		raise RuntimeError("nope")

	with TestMrsalBroker(consumer) as br:
		_register_sync(br, boom, dlx_enable=False)
		br.publish(b"to be dropped", exchange="ex", routing_key="rk")

	dropped = handler.with_outcome("dropped")
	assert len(dropped) == 1
	assert dropped[0].queue == "q"


def test_retry_cycle_disposition_carries_outcome_retry():
	# Retry-cycle republish to the .retry queue logs outcome='retry' (m2).
	logger, handler = _make_logger("test.mrsal.retry")
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_logger(logger)

	def boom(mf, props, body):
		raise RuntimeError("deliberate failure")

	with TestMrsalBroker(consumer) as br:
		_register_sync(br, boom, dlx_enable=True, enable_retry_cycles=True,
			retry_backoff="fixed", retry_cycle_interval=10, max_retry_time_limit=8 * 60)
		br.publish(b"cycle me", exchange="ex", routing_key="rk")

	retried = handler.with_outcome("retry")
	assert len(retried) == 1
	assert retried[0].queue == "q"


def test_retry_cycle_terminal_park_carries_outcome_dlx():
	# When the budget is exhausted the message parks in the terminal DLX -> outcome='dlx'.
	logger, handler = _make_logger("test.mrsal.parked")
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	consumer.set_logger(logger)

	def boom(mf, props, body):
		raise RuntimeError("deliberate failure")

	with TestMrsalBroker(consumer) as br:
		_register_sync(br, boom, dlx_enable=True, enable_retry_cycles=True,
			max_retry_time_limit=0, retry_cycle_interval=1)
		br.publish(b"to be parked", exchange="ex", routing_key="rk")

	assert len(handler.with_outcome("dlx")) == 1


def test_set_logger_none_falls_back_to_module_logger(caplog):
	# Unset logger: records flow to the module logger (mrsal.amqp.subclass).
	consumer = MrsalBlockingAMQP(**SYNC_ARGS)
	assert consumer._logger is None

	with caplog.at_level(logging.INFO, logger="mrsal.amqp.subclass"):
		with TestMrsalBroker(consumer) as br:
			_register_sync(br, lambda mf, props, body: None)
			br.publish(b"ok", exchange="ex", routing_key="rk")

	processed = [r for r in caplog.records if getattr(r, "outcome", None) == "processed"]
	assert len(processed) == 1
	assert processed[0].name == "mrsal.amqp.subclass"
