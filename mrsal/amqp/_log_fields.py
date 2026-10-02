"""Structured ``extra={...}`` field sets for consume and publish log records.

Shared by the blocking classes in ``mrsal.amqp.subclass`` and ``MrsalAsyncAMQP``
in ``mrsal.amqp.async_amqp``.
"""
import time


def _consume_log_extra(*, msg_id, app_id, queue, routing_key, retry, outcome, start_ts):
	"""Build the structured field set attached to a consume-lifecycle log record.

	Passed as the stdlib ``logging`` ``extra={...}`` so a backend can filter on
	discrete fields instead of regex-parsing the prose message. ``outcome`` is
	one of ``processed`` / ``validation_failed`` / ``callback_failed`` / ``dlx``
	/ ``dropped``.

	``duration_ms`` is receipt-to-this-record wall-clock (delivery received ->
	log call), **not** the handler duration -- so it is not the same number as
	the ``on_consume`` metrics hook (which times validation+callback only). It
	also brackets the disposition slightly differently across paths: the sync
	terminal records are emitted before the ack/nack is scheduled, while the
	async ones are emitted after ``message.ack()``/``reject()`` is awaited.
	"""
	return {
			'msg_id': msg_id,
			'app_id': app_id,
			'queue': queue,
			'routing_key': routing_key,
			'retry': retry,
			'outcome': outcome,
			'duration_ms': round((time.monotonic() - start_ts) * 1000, 2),
			}


def _publish_log_extra(*, exchange, routing_key, outcome):
	"""Build the structured field set attached to a publish log record.

	``outcome`` is ``published`` or ``failed``.
	"""
	return {
			'exchange': exchange,
			'routing_key': routing_key,
			'outcome': outcome,
			}
