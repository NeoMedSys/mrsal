# external
import logging
from dataclasses import dataclass
from typing import Callable

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class MetricsHooks:
	"""Push-based instrumentation callbacks (parity with sonic's ``types.MetricsHooks``).

	Any subset may be set; an unset hook costs a single ``None`` check on the
	hot path. Hooks run synchronously where the event occurs, so the contract
	is **fast, non-blocking, no exceptions** -- offload to a background task if
	the host needs async export.

	Install via ``Mrsal.set_metrics_hooks``; pass ``None`` to clear.

	:param on_publish: fired once per publish operation. ``success`` is False on
		broker rejection (``NackError`` / ``UnroutableError`` / confirm timeout).
		``duration_s`` spans entry to broker ack/nack. NOT recovered if it raises
		-- mirrors sonic, where an ``OnPublish`` panic propagates.
	:param on_consume: fired exactly once per delivery after the handler returns.
		``success`` is True only when payload validation passed and the callback
		returned without raising; ``duration_s`` spans validation plus the
		callback. A message that failed but was routed to DLX still reports
		``success=False`` -- the DLX outcome is ``on_retry`` / ``on_dlx_final``.
	:param on_retry: fired when a failed delivery is republished to the
		``<queue>.retry`` queue. ``cycle`` is the cycle the message is entering;
		``delay_s`` is the next retry delay.
	:param on_dlx_final: fired when a message is parked in the terminal
		``<queue>.dlx`` queue with ``x-retry-exhausted=True``. ``queue_name`` is
		the terminal queue name.
	"""
	on_publish: Callable[[bool, float], None] | None = None
	on_consume: Callable[[bool, float], None] | None = None
	on_retry: Callable[[int, float], None] | None = None
	on_dlx_final: Callable[[str], None] | None = None


def safe_invoke(hook: Callable | None, *args) -> None:
	"""Invoke a consume-path hook, swallowing and logging any exception.

	Mirrors sonic's per-delivery guard for ``OnConsume`` / ``OnRetry`` /
	``OnDLXFinal``: a misbehaving hook must never kill the consumer. ``on_publish``
	is deliberately NOT routed through here -- sonic does not recover an
	``OnPublish`` panic, so it propagates to the publish caller.
	"""
	if hook is None:
		return
	try:
		hook(*args)
	except Exception:
		log.warning("metrics hook raised; ignoring", exc_info=True)
