import asyncio
import json
import logging
import time
from datetime import timedelta
from mrsal.exceptions import MrsalAbortedSetup, MrsalDLXPublishTimeout, MrsalNoAsyncioLoopError
from pika.exceptions import (
		AMQPConnectionError,
		ChannelClosedByBroker,
		StreamLostError,
		ConnectionClosedByBroker,
		)
from aio_pika import connect_robust, Message
from aio_pika.exceptions import AuthenticationError
from dataclasses import field
from typing import Any, Callable, Literal, Sequence, Type
from tenacity import AsyncRetrying, RetryCallState, wait_exponential, retry_if_exception_type, retry_if_not_exception_type
from pydantic import ConfigDict, ValidationError
from pydantic.dataclasses import dataclass

from mrsal.superclass import Mrsal, _ASYNC_CONNECTION_ERRORS
from mrsal.amqp._log_fields import _consume_log_extra
from mrsal import config

# Kept from before the split (#108), so log filters on the old name still match.
# A compatibility contract (see README, 3.16.0): do not change it to __name__.
log = logging.getLogger("mrsal.amqp.subclass")


# MrsalAsyncAMQP.start_consumer rebuilds on a lost connection. Refused
# credentials are a builtin ConnectionError too, but retrying cannot fix them:
# they raise at once, as before #105.
_CONSUMER_RETRY = retry_if_exception_type((
	AMQPConnectionError,
	ChannelClosedByBroker,
	ConnectionClosedByBroker,
	StreamLostError,
	*_ASYNC_CONNECTION_ERRORS,
	)) & retry_if_not_exception_type(AuthenticationError)
_CONSUMER_RETRY_WAIT = wait_exponential(multiplier=1, min=2, max=60)


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class MrsalAsyncAMQP(Mrsal):
	"""Handles asynchronous connection with RabbitMQ using aio-pika."""

	# Seconds the DLX publish may take before the delivery is rejected instead (#105).
	dlx_publish_timeout: float = config.DEFAULT_DLX_PUBLISH_TIMEOUT_SEC
	_dlx_publish_channel: Any = field(init=False, default=None)
	_stop_event: asyncio.Event | None = field(init=False, default=None)
	# aio_pika.queue.QueueIterator at runtime; typed as object to avoid importing
	# the internal queue module just for a type hint.
	_consumer_iterator: object | None = field(init=False, default=None)
	_inflight_tasks: set[asyncio.Task] | None = field(init=False, default=None)

	async def stop(self) -> None:
		"""Signal the consumer loop to exit cleanly.

		Sets ``_stop_event`` so the loop breaks at its next iteration and
		closes the active queue iterator so an idle consumer wakes up
		instead of hanging on the broker. Safe to call multiple times.

		Any in-flight messages dispatched via ``max_concurrent_tasks`` are
		drained by ``start_consumer`` before it returns.

		Note: once stop() has been called, this consumer instance cannot be
		restarted -- ``_stop_event`` remains set so future ``start_consumer``
		calls would exit on the first iteration. To restart, construct a new
		``MrsalAsyncAMQP`` instance. The persistent set state is deliberate:
		it preserves a stop request that arrives during a tenacity retry
		backoff, which would otherwise be silently dropped.
		"""
		_log = self._logger or log
		if self._stop_event is not None:
			self._stop_event.set()
		if self._consumer_iterator is not None:
			try:
				await self._consumer_iterator.close()
			except Exception:
				_log.debug("Consumer iterator close raised; ignoring.", exc_info=True)

	async def close(self) -> None:
		"""Close channels and connection cleanly, ending a running consumer.

		Sets the stop event first, so a running ``start_consumer`` sees a
		deliberate shutdown and returns instead of treating the close as a
		connection loss and reconnecting. As with ``stop()``, a consumer that has
		run on this instance cannot be restarted afterwards.
		"""
		if self._stop_event is not None:
			self._stop_event.set()
		await self._close_handles()

	async def _close_handles(self) -> None:
		"""Close channels and connection without signalling a stop.

		Each close is bounded and wrapped: on a half-dead connection a close can
		hang, and a failure on one handle must not leak the next.
		"""
		if self._dlx_publish_channel is not None and not self._dlx_publish_channel.is_closed:
			await self._close_bounded(handle=self._dlx_publish_channel, name="DLX publish channel")
		self._dlx_publish_channel = None

		if self._channel is not None and not self._channel.is_closed:
			await self._close_bounded(handle=self._channel, name="Consumer channel")
		self._channel = None

		if self._connection is not None and not self._connection.is_closed:
			await self._close_bounded(handle=self._connection, name="Connection")
		self._connection = None

	async def _close_bounded(self, handle, name: str) -> None:
		"""Close ``handle`` within ``config.CLOSE_TIMEOUT_SEC``; log and move on if it fails."""
		_log = self._logger or log
		try:
			await asyncio.wait_for(handle.close(), timeout=config.CLOSE_TIMEOUT_SEC)
		except asyncio.TimeoutError:
			_log.warning(f"{name} close did not finish within {config.CLOSE_TIMEOUT_SEC}s; dropping it.")
		except Exception:
			_log.warning(f"{name} close raised; dropping it.", exc_info=True)

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc_val, exc_tb):
		await self.close()
		return False

	async def _ensure_async_connection(self) -> None:
		"""Idempotent: only connects if not already connected. Closes stale connections."""
		_log = self._logger or log
		if self._connection is None or self._connection.is_closed:
			if self._connection is not None:
				try:
					await self._connection.close()
				except Exception:
					_log.debug("Stale connection close raised; ignoring.", exc_info=True)
				self._connection = None
			await self.setup_async_connection()

	async def _ensure_consumer_channel(self) -> None:
		"""Close any prior open channel before opening a new one (prevents leak on tenacity retry).

		Not safe for concurrent callers; start_consumer is the only call site.
		"""
		_log = self._logger or log
		if self._channel is not None and not self._channel.is_closed:
			try:
				await self._channel.close()
			except Exception:
				_log.debug("Stale channel close raised; ignoring.", exc_info=True)
			self._channel = None
		channel = await self._connection.channel()
		try:
			await channel.set_qos(prefetch_count=self.prefetch_count)
		except Exception:
			await channel.close()
			raise
		self._channel = channel

	async def _ensure_dlx_publish_channel(self) -> None:
		"""Lazily open a dedicated channel with publisher confirms for DLX writes.

		Why: publisher confirms make ``exchange.publish(...)`` await a broker
		ack and raise (e.g. ``aio_pika.exceptions.DeliveryError``) on negative
		ack or connection loss. Without confirms a dropped DLX publish would
		return successfully and the caller would ack the original message,
		causing silent message loss.

		Kept separate from the consumer channel so confirms semantics don't
		affect the consume path. Not safe for concurrent callers; the consume
		loop serializes DLX publishes today.
		"""
		_log = self._logger or log
		await self._ensure_async_connection()
		if self._dlx_publish_channel is not None and not self._dlx_publish_channel.is_closed:
			return
		if self._dlx_publish_channel is not None:
			try:
				await self._dlx_publish_channel.close()
			except Exception:
				_log.debug("Stale DLX publish channel close raised; ignoring.", exc_info=True)
			self._dlx_publish_channel = None
		self._dlx_publish_channel = await self._connection.channel(publisher_confirms=True)

	async def setup_async_connection(self):
		"""Setup an asynchronous connection to RabbitMQ using aio-pika."""
		_log = self._logger or log
		_log.info(f"Establishing async connection to RabbitMQ on {self.host}:{self.port}")
		try:
			self._connection = await connect_robust(
				host=self.host,
				port=self.port,
				login=self.credentials[0],
				password=self.credentials[1],
				virtualhost=self.virtual_host,
				ssl=self.ssl,
				ssl_context=self.get_ssl_context(),
				heartbeat=self.heartbeat
			)
			_log.info("Async connection established successfully.")
		except (AMQPConnectionError, StreamLostError, ChannelClosedByBroker, ConnectionClosedByBroker) as e:
			_log.error(f"Error establishing async connection: {e}", exc_info=True)
			raise
		except Exception as e:
			_log.error(f'Oh my lordy lord! I caugth an unexpected exception while trying to connect: {e}', exc_info=True)
			raise

	async def _handle_message(self, message, runtime_config: dict) -> None:
		"""Process a single message: validate -> callback -> ack/DLX.

		Shared by the sequential and concurrent paths in ``start_consumer`` so
		the failure/ack policy stays in one place regardless of dispatch mode.
		"""
		_log = self._logger or log
		callback = runtime_config['callback']
		callback_args = runtime_config['callback_args']
		auto_ack = runtime_config['auto_ack']
		payload_model = runtime_config['payload_model']
		dlx_enable = runtime_config['dlx_enable']
		enable_retry_cycles = runtime_config['enable_retry_cycles']
		retry_cycle_interval = runtime_config['retry_cycle_interval']
		max_retry_time_limit = runtime_config['max_retry_time_limit']
		retry_backoff = runtime_config['retry_backoff']
		retry_backoff_max = runtime_config['retry_backoff_max']
		exchange_name = runtime_config['exchange_name']
		routing_key = runtime_config['routing_key']
		dlx_exchange_name = runtime_config['dlx_exchange_name']
		dlx_routing_key = runtime_config['dlx_routing_key']
		queue_name = runtime_config['queue_name']

		app_id = 'NoAppID' if message.app_id is None else message.app_id
		msg_id = 'NoMsgID' if message.message_id is None else message.message_id
		properties = config.AioPikaAttributes.from_message(message)

		if self.verbose:
			_log.info(f"""
						Message received with:
						- Redelivery: {message.redelivered}
						- Exchange: {message.exchange}
						- Routing Key: {message.routing_key}
						- Delivery Tag: {message.delivery_tag}
						- Auto Ack: {auto_ack}
						""")

		current_retry = message.headers.get('x-delivery-count', 0) if message.headers else 0
		start_ts = time.monotonic()

		def fields(outcome):
			return _consume_log_extra(
				msg_id=msg_id, app_id=app_id, queue=queue_name, routing_key=message.routing_key,
				retry=current_retry, outcome=outcome, start_ts=start_ts)

		should_process = True
		failure_reason: str | None = None
		# When payload_model is set, the validated instance replaces message.body in the callback.
		callback_body = message.body

		with self._measure_consume() as outcome:
			if payload_model:
				try:
					callback_body = self.validate_payload(payload=message.body, model=payload_model)
				except (ValidationError, json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
					_log.error(f"Payload validation failed: {e}", exc_info=True, extra=fields('validation_failed'))
					should_process = False
					failure_reason = f"payload validation: {e!r}"

			if callback and should_process:
				try:
					if callback_args:
						await callback(*callback_args, message, properties, callback_body)
					else:
						await callback(message, properties, callback_body)
				except Exception as e:
					_log.error(f"Splæt! Error processing message with callback: {e}", exc_info=True, extra=fields('callback_failed'))
					should_process = False
					failure_reason = f"callback: {e!r}"

			# End of the validation+callback span the on_consume duration measures.
			outcome.ok = should_process
			outcome.mark_handler_done()

			# auto_ack=True: broker already acked; skip DLX (caller opted out of accountability)
			if auto_ack:
				if not should_process:
					_log.warning(
						f"Message {msg_id} dropped (auto_ack=True): {failure_reason} | "
						f"app_id={app_id} routing_key={message.routing_key}",
						extra=fields('dropped'))
				return

			if not should_process:
				if dlx_enable and enable_retry_cycles:
					await self._async_publish_to_dlx_with_retry_cycle(
						message, properties, failure_reason or "Callback processing failed",
						exchange_name, routing_key, enable_retry_cycles,
						retry_cycle_interval, max_retry_time_limit, dlx_exchange_name,
						dlx_routing_key,
						retry_backoff, retry_backoff_max,
						queue_name,
					)
				elif dlx_enable:
					await message.reject(requeue=False)
					_log.warning(f"Message {msg_id} sent to dead letter exchange after {current_retry} retries", extra=fields('dlx'))
				else:
					await message.reject(requeue=False)
					_log.warning(f"No dead letter exchange for {queue_name} declared, proceeding to drop the message -- Ponder you life choices! byebye", extra=fields('dropped'))
					if self.verbose:
						_log.info(f"Dropped message content: {message.body}")
				return

			await message.ack()
			_log.info(f'Young grasshopper! Message ({msg_id}) from {app_id} received and properly processed.', extra=fields('processed'))

	async def _handle_message_with_release(self, message, runtime_config: dict,
										semaphore: asyncio.Semaphore) -> None:
		"""Task body for concurrent dispatch: handle one message and release the slot.

		Wrapped so a crash inside ``_handle_message`` cannot leak the semaphore
		permit or kill the parent loop. Errors are logged; the iterator keeps
		moving. A connection error is re-raised: the consume loop's done-callback
		turns it into a connection loss so the consumer is rebuilt (#105).
		"""
		_log = self._logger or log
		try:
			await self._handle_message(message, runtime_config)
		except _ASYNC_CONNECTION_ERRORS:
			raise
		except Exception:
			_log.exception("Unhandled error processing message in concurrent task")
		finally:
			semaphore.release()

	async def start_consumer(
			self,
			queue_name: str,
			callback: Callable | None = None,
			callback_args: Sequence[str | int | float | bool] | None = None,
			auto_ack: bool = False,
			auto_declare: bool = True,
			exchange_name: str | None = None,
			exchange_type: str | None = None,
			routing_key: str | None = None,
			payload_model: Type | None = None,
			dlx_enable: bool = True,
			dlx_exchange_name: str | None = None,
			dlx_routing_key: str | None = None,
			use_quorum_queues: bool = True,
			enable_retry_cycles: bool = True,
			retry_cycle_interval: int = config.DEFAULT_RETRY_CYCLE_INTERVAL_MIN,
			max_retry_time_limit: int = config.DEFAULT_MAX_RETRY_TIME_LIMIT_MIN,
			retry_backoff: Literal["fixed", "exponential"] = config.DEFAULT_RETRY_BACKOFF,
			retry_backoff_max: int = config.DEFAULT_RETRY_BACKOFF_MAX_MIN,
			max_queue_length: int | None = None,
			max_queue_length_bytes: int | None = None,
			queue_overflow: str | None = None,
			single_active_consumer: bool | None = None,
			lazy_queue: bool | None = None,
			max_concurrent_tasks: int | None = None,
			drain_timeout: float | None = None,
			):
		"""Start the async consumer.

		Runs until ``stop()`` / ``close()``. A lost connection or channel ends the
		consume loop and the retry rebuilds connection, channel, topology and
		consumer with exponential backoff (2s up to 60s). The retry has no stop
		condition: against a permanently unreachable broker it retries forever,
		logging each attempt at WARNING. That suits a long-running service, which
		should alert on those warnings rather than expect ``start_consumer`` to raise.
		Refused credentials (``aio_pika.exceptions.AuthenticationError``) are not
		retried and raise at once.
		A ``stop()`` / ``close()`` during a backoff ends it at once.

		:param str queue_name: The queue to consume from
		:param Callable callback: Async callable invoked as ``callback(*callback_args, message, properties, body)``.
			When ``payload_model`` is set, ``body`` is the validated model instance, not ``message.body``.
		:param Sequence callback_args: Optional positional arguments prepended to the callback invocation
		:param bool auto_ack: If True, the broker acks at delivery (``no_ack=True``). Rejected at
			setup when combined with ``dlx_enable=True`` -- once the broker has acked, failed
			messages cannot be routed to the DLX, so the combination is meaningless. To use
			auto_ack, pass ``dlx_enable=False`` explicitly and accept that callback/validation
			failures are logged and dropped. Default False. WARNING: ``no_ack=true`` makes the
			broker ignore ``prefetch_count`` and push deliveries as fast as the connection
			allows; aio-pika buffers them in an unbounded internal queue, so a slow callback
			on a busy stream can OOM the process. ``max_concurrent_tasks`` does not bound that
			buffer. Treat this mode as unsafe for production -- use ``auto_ack=False`` if you
			need backpressure.
		:param bool auto_declare: If True, declare exchange/queue before consuming
		:param str exchange_name: Exchange name for auto_declare
		:param str exchange_type: Exchange type for auto_declare
		:param str routing_key: Routing key for auto_declare
		:param Type payload_model: Pydantic model for payload validation. When set, validation
			failures route to DLX before ``callback`` runs, and the callback receives the
			validated model instance in place of ``message.body``.
		:param bool dlx_enable: Whether to route failed messages to a dead-letter exchange
		:param bool enable_retry_cycles: Whether to apply retry cycle headers when publishing to DLX
		:param str retry_backoff: "fixed" (flat queue-TTL interval) or "exponential"
			(per-message ``base * 2**cycle`` clamped at ``retry_backoff_max`` with ±20% jitter).
			Default "exponential". Switching modes on a running deployment requires deleting
			the existing ``<queue>.retry`` queue (the ``x-message-ttl`` arg becomes inequivalent).
		:param int retry_backoff_max: Per-cycle ceiling in minutes for exponential mode.
			Ignored when ``retry_backoff="fixed"``. Default 60.
		:param int max_concurrent_tasks: When set, up to N messages are processed concurrently as
			``asyncio`` tasks bounded by a semaphore. When ``None`` (default), messages are processed
			sequentially -- one ``await callback(...)`` at a time -- matching prior behaviour. Note that
			``prefetch_count`` only buffers messages on the broker side; it does not parallelize the
			consumer. Combine with ``prefetch_count >= max_concurrent_tasks`` for steady throughput.
		:param float drain_timeout: Seconds to wait for in-flight tasks to finish after the loop
			exits (graceful stop or end-of-iteration). ``None`` (default) waits indefinitely. When
			the timeout fires, remaining tasks are cancelled and the consumer returns; messages
			handled by cancelled tasks will be redelivered by the broker.
		"""
		# Created here, before the first attempt, so stop() / close() can land
		# while the retry is still trying to connect (the loop may never have run).
		_log = self._logger or log
		if self._stop_event is None:
			self._stop_event = asyncio.Event()

		def _log_retry(retry_state: RetryCallState) -> None:
			_log.warning(
				f"start_consumer({queue_name}): retrying in {retry_state.upcoming_sleep:.1f}s "
				f"after {retry_state.outcome.exception()!r}"
			)

		async for attempt in AsyncRetrying(
				retry=_CONSUMER_RETRY,
				wait=_CONSUMER_RETRY_WAIT,
				before_sleep=_log_retry,
				sleep=self._sleep_unless_stopped):
			with attempt:
				# A stop that landed during the backoff ends the consumer here,
				# before a new connection is opened.
				if self._stop_event.is_set():
					_log.info(f"start_consumer({queue_name}): stop() / close() already called on this instance; not starting.")
					return
				try:
					queue, runtime_config = await self._prepare_consumer_async(
						queue_name=queue_name,
						callback=callback,
						callback_args=callback_args,
						auto_ack=auto_ack,
						auto_declare=auto_declare,
						exchange_name=exchange_name,
						exchange_type=exchange_type,
						routing_key=routing_key,
						payload_model=payload_model,
						dlx_enable=dlx_enable,
						dlx_exchange_name=dlx_exchange_name,
						dlx_routing_key=dlx_routing_key,
						use_quorum_queues=use_quorum_queues,
						enable_retry_cycles=enable_retry_cycles,
						retry_cycle_interval=retry_cycle_interval,
						max_retry_time_limit=max_retry_time_limit,
						retry_backoff=retry_backoff,
						retry_backoff_max=retry_backoff_max,
						max_queue_length=max_queue_length,
						max_queue_length_bytes=max_queue_length_bytes,
						queue_overflow=queue_overflow,
						single_active_consumer=single_active_consumer,
						lazy_queue=lazy_queue,
						max_concurrent_tasks=max_concurrent_tasks,
					)
					# A close() that landed while prepare was connecting could not close a
					# connection that was not assigned yet; close it now.
					if self._stop_event.is_set():
						await self._close_handles()
						return
					await self._run_consume_loop_async(
						queue=queue,
						runtime_config=runtime_config,
						auto_ack=auto_ack,
						max_concurrent_tasks=max_concurrent_tasks,
						drain_timeout=drain_timeout,
					)
				except _ASYNC_CONNECTION_ERRORS:
					# Lost in setup or in the loop. A robust connection mid-reconnect still
					# reports is_closed=False, so _ensure_async_connection would reuse it;
					# drop the handles (without signalling a stop) so the retry connects fresh.
					# A stop already requested ends here instead of waiting out a backoff.
					await self._close_handles()
					if self._stop_event.is_set():
						return
					raise

	async def _sleep_unless_stopped(self, seconds: float) -> None:
		"""start_consumer's retry backoff; ends early on ``stop()`` / ``close()``."""
		try:
			await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
		except asyncio.TimeoutError:
			# No stop during the backoff: the full sleep elapsed, retry now.
			pass

	async def _prepare_consumer_async(
			self,
			*,
			queue_name: str,
			callback: Callable | None,
			callback_args: Sequence[str | int | float | bool] | None,
			auto_ack: bool,
			auto_declare: bool,
			exchange_name: str | None,
			exchange_type: str | None,
			routing_key: str | None,
			payload_model: Type | None,
			dlx_enable: bool,
			dlx_exchange_name: str | None,
			dlx_routing_key: str | None,
			use_quorum_queues: bool,
			enable_retry_cycles: bool,
			retry_cycle_interval: int,
			max_retry_time_limit: int,
			retry_backoff: Literal["fixed", "exponential"],
			retry_backoff_max: int,
			max_queue_length: int | None,
			max_queue_length_bytes: int | None,
			queue_overflow: str | None,
			single_active_consumer: bool | None,
			lazy_queue: bool | None,
			max_concurrent_tasks: int | None,
	) -> tuple[Any, dict]:
		"""Validate config, ensure the async connection/channel, declare topology,
		and return ``(queue, runtime_config)``.

		Split out of ``start_consumer`` so the setup and the consume loop can be
		driven separately: the in-memory test broker (``mrsal.testing``) calls
		this to register a consumer and then delivers messages by hand, without
		entering ``_run_consume_loop_async``. Behaviour for the normal
		``start_consumer`` path is unchanged.
		"""
		_log = self._logger or log
		if auto_ack and dlx_enable:
			raise MrsalAbortedSetup(
				'auto_ack=True is incompatible with dlx_enable=True: once the broker has acked '
				'on delivery, failed messages cannot be routed to the DLX. Set dlx_enable=False '
				'to opt out of DLX, or auto_ack=False to keep DLX accountability.'
			)
		if enable_retry_cycles and dlx_enable:
			self._validate_retry_cycle_preconditions(
				exchange_type=exchange_type,
				routing_key=routing_key,
				retry_cycle_interval=retry_cycle_interval,
				auto_declare=auto_declare,
				retry_backoff=retry_backoff,
				retry_backoff_max=retry_backoff_max,
			)

		try:
			asyncio.get_running_loop()
		except RuntimeError:
			raise MrsalNoAsyncioLoopError('Young grasshopper! You forget to add asyncio.run(mrsal.start_consumer(...))')
		await self._ensure_async_connection()
		await self._ensure_consumer_channel()

		if auto_declare:
			if None in (exchange_name, queue_name, exchange_type, routing_key):
				raise TypeError('Make sure that you are passing in all the necessary args for auto_declare')

			queue = await self._async_setup_exchange_and_queue(
					exchange_name=exchange_name,
					queue_name=queue_name,
					exchange_type=exchange_type,
					routing_key=routing_key,
					dlx_enable=dlx_enable,
					dlx_exchange_name=dlx_exchange_name,
					dlx_routing_key=dlx_routing_key,
					use_quorum_queues=use_quorum_queues,
					max_queue_length=max_queue_length,
					max_queue_length_bytes=max_queue_length_bytes,
					queue_overflow=queue_overflow,
					single_active_consumer=single_active_consumer,
					lazy_queue=lazy_queue,
					enable_retry_cycles=enable_retry_cycles,
					retry_cycle_interval=retry_cycle_interval,
					retry_backoff=retry_backoff,
					retry_backoff_max=retry_backoff_max,
					)

			if not self.auto_declare_ok:
				# Not close(): that would set the stop flag and silently end every
				# later start_consumer on this instance.
				await self._close_handles()
				raise MrsalAbortedSetup('Auto declaration failed during setup.')
		else:
			# The async consume loop needs the declared aio_pika queue object to
			# open its iterator; without auto_declare there is no queue to return.
			# Fail with a clear message instead of a later UnboundLocalError.
			raise MrsalAbortedSetup(
				'The async consumer requires auto_declare=True: it needs the declared '
				'aio_pika queue object to open the consume iterator. Declaring topology '
				'out of band (auto_declare=False) is not supported on the async path.'
			)

		# Log consumer configuration
		consumer_config = {
			"queue": queue_name,
			"exchange": exchange_name,
			"max_length": max_queue_length or self.max_queue_length,
			"overflow": queue_overflow or self.queue_overflow,
			"single_consumer": single_active_consumer if single_active_consumer is not None else self.single_active_consumer,
			"lazy": lazy_queue if lazy_queue is not None else self.lazy_queue,
			"max_concurrent_tasks": max_concurrent_tasks,
		}

		_log.info(f"Straight out of the swamps -- consumer boi listening with config: {consumer_config}")

		runtime_config = {
			'callback': callback,
			'callback_args': callback_args,
			'auto_ack': auto_ack,
			'payload_model': payload_model,
			'dlx_enable': dlx_enable,
			'enable_retry_cycles': enable_retry_cycles,
			'retry_cycle_interval': retry_cycle_interval,
			'max_retry_time_limit': max_retry_time_limit,
			'retry_backoff': retry_backoff,
			'retry_backoff_max': retry_backoff_max,
			'exchange_name': exchange_name,
			'routing_key': routing_key,
			'dlx_exchange_name': dlx_exchange_name,
			'dlx_routing_key': dlx_routing_key,
			'queue_name': queue_name,
		}
		return queue, runtime_config

	async def _run_consume_loop_async(
			self,
			*,
			queue: Any,
			runtime_config: dict,
			auto_ack: bool,
			max_concurrent_tasks: int | None,
			drain_timeout: float | None,
	) -> None:
		"""Drive the async consume loop using a prepared ``queue``/``runtime_config``."""
		_log = self._logger or log
		if self._inflight_tasks is None:
			self._inflight_tasks = set()
		if max_concurrent_tasks is not None and max_concurrent_tasks > 0:
			semaphore = asyncio.Semaphore(max_concurrent_tasks)
		else:
			semaphore = None

		# Set when the connection or consumer channel closes. aio-pika's robust
		# reconnect can fail to restore the channel and leave the iterator waiting
		# forever (#105), so a loss ends the loop and the start_consumer retry
		# rebuilds connection, channel, topology and consumer from scratch.
		connection_lost: asyncio.Future = asyncio.get_running_loop().create_future()

		def _mark_lost(_sender=None, exc: BaseException | None = None) -> None:
			"""Close callback (aio-pika calls it with sender and exc) and task-error path."""
			if not connection_lost.done():
				connection_lost.set_result(exc)

		def _on_task_done(task: asyncio.Task) -> None:
			self._inflight_tasks.discard(task)
			if not task.cancelled() and isinstance(task.exception(), _ASYNC_CONNECTION_ERRORS):
				_mark_lost(exc=task.exception())

		connection, channel = self._connection, self._channel
		connection.close_callbacks.add(_mark_lost)
		channel.close_callbacks.add(_mark_lost)
		stopped = asyncio.ensure_future(self._stop_event.wait())

		try:
			# async with: ensures the consumer cancellation is deterministically delivered
			# to the broker on exception or generator GC. Without it, channel state can
			# be left mid-cancel.
			async with queue.iterator(no_ack=auto_ack) as it:
				self._consumer_iterator = it
				async for message in self._until_connection_lost(it=it, connection_lost=connection_lost, stopped=stopped):
					# Stop check runs BEFORE processing the message we just pulled.
					# Trade-off: a stop arriving between pulls leaves the just-pulled
					# message unacked, which the broker redelivers on consumer cancel.
					# Alternative (process-then-check) would risk an unbounded delay
					# before stop() takes effect when callbacks are slow.
					if self._stop_event.is_set():
						break
					if message is None:
						continue

					if semaphore is None:
						# Sequential path -- preserves prior behaviour exactly.
						await self._handle_message(message, runtime_config)
					else:
						# Bounded concurrent path. acquire() applies back-pressure so the
						# iterator stops pulling new messages once max_concurrent_tasks
						# are in flight, even if prefetch_count is larger.
						await semaphore.acquire()
						if self._stop_event.is_set():
							semaphore.release()
							break
						task = asyncio.create_task(
							self._handle_message_with_release(message, runtime_config, semaphore)
						)
						self._inflight_tasks.add(task)
						# Keeps the in-flight set bounded, and turns a task's connection
						# error into a connection loss.
						task.add_done_callback(_on_task_done)
		finally:
			stopped.cancel()
			self._consumer_iterator = None
			if self._inflight_tasks:
				# Drain in-flight messages before returning so callers observing
				# start_consumer() returning can trust that no work is still pending.
				# gather() swallows individual task exceptions (already logged inside
				# _handle_message_with_release).
				pending = list(self._inflight_tasks)
				_log.info(f"Draining {len(pending)} in-flight message task(s) before exit")
				drain_coro = asyncio.gather(*pending, return_exceptions=True)
				if drain_timeout is None:
					await drain_coro
				else:
					try:
						await asyncio.wait_for(drain_coro, timeout=drain_timeout)
					except asyncio.TimeoutError:
						still_pending = [t for t in pending if not t.done()]
						_log.warning(
							f"Drain timeout after {drain_timeout}s: cancelling "
							f"{len(still_pending)} unfinished task(s); their messages "
							f"will be redelivered by the broker."
						)
						for task in still_pending:
							task.cancel()
						await asyncio.gather(*still_pending, return_exceptions=True)
			connection.close_callbacks.discard(_mark_lost)
			channel.close_callbacks.discard(_mark_lost)

	@staticmethod
	async def _until_connection_lost(it, connection_lost: asyncio.Future, stopped: asyncio.Future):
		"""Yield from the queue iterator until it ends, a stop, or a connection loss.

		Returns on a stop (``stop()`` / ``close()``), which wins over a loss
		seen at the same time. Raises ``ConnectionError`` (retriable by
		``start_consumer``) when ``connection_lost`` resolves while waiting for
		the next message.
		"""
		while True:
			next_message = asyncio.ensure_future(it.__anext__())
			await asyncio.wait({next_message, connection_lost, stopped}, return_when=asyncio.FIRST_COMPLETED)
			if not next_message.done():
				next_message.cancel()
				# Own the cancelled pull: its close() on a dead channel may raise.
				await asyncio.gather(next_message, return_exceptions=True)
				if stopped.done():
					return
				raise ConnectionError(f"Consumer connection lost: {connection_lost.result()!r}")
			try:
				message = next_message.result()
			except StopAsyncIteration:
				# The iterator also ends when its channel dies; that is a loss, not
				# a clean end, unless a stop was requested.
				if connection_lost.done() and not stopped.done():
					raise ConnectionError(f"Consumer connection lost: {connection_lost.result()!r}")
				return
			yield message

	async def _async_publish_to_dlx_with_retry_cycle(self, message, properties, processing_error: str,
												original_exchange: str, original_routing_key: str,
												enable_retry_cycles: bool, retry_cycle_interval: int,
												max_retry_time_limit: int, dlx_exchange_name: str | None,
												dlx_routing_key: str | None = None,
												retry_backoff: Literal["fixed", "exponential"] = config.DEFAULT_RETRY_BACKOFF,
												retry_backoff_max: int = config.DEFAULT_RETRY_BACKOFF_MAX_MIN,
												queue_name: str | None = None):
		"""Async publish message to DLX with retry cycle headers.

		At-least-once delivery for DLX: the publish uses publisher confirms on
		a dedicated channel, so broker rejection or connection loss raises and
		the original message is rejected (not acked). If the process crashes
		between the confirmed DLX publish and the original ack, the message
		will be redelivered and re-published to DLX. A ``dlx_publish_timeout``
		gives up waiting for the confirm, not the publish: if the broker did
		accept it, the rejected original is dead-lettered too, leaving one copy
		with retry headers and one without. Consumers must be idempotent.
		"""
		_log = self._logger or log
		msg_id = getattr(properties, 'message_id', 'unknown')
		app_id = getattr(properties, 'app_id', 'unknown')
		try:
			# Use common logic from superclass
			await self._handle_dlx_with_retry_cycle_async(
				message=message,
				properties=properties,
				processing_error=processing_error,
				original_exchange=original_exchange,
				original_routing_key=original_routing_key,
				enable_retry_cycles=enable_retry_cycles,
				retry_cycle_interval=retry_cycle_interval,
				max_retry_time_limit=max_retry_time_limit,
				dlx_exchange_name=dlx_exchange_name,
				dlx_routing_key=dlx_routing_key,
				retry_backoff=retry_backoff,
				retry_backoff_max=retry_backoff_max,
				queue_name=queue_name,
			)

			# Acknowledge original message
			await message.ack()
			
		except _ASYNC_CONNECTION_ERRORS as e:
			# Connection gone: leave the message unsettled; the broker redelivers it
			# when the channel closes (#105). Re-raised so the consume loop rebuilds
			# even when only the DLX channel died and no close callback fires.
			_log.error(f"Failed to send message to DLX, connection lost; leaving unsettled for redelivery: {e} | message_id={msg_id} app_id={app_id} delivery_tag={message.delivery_tag} exchange={original_exchange} routing_key={original_routing_key}")
			raise
		except MrsalDLXPublishTimeout:
			# Connection still up but the DLX publish never completed: settle the
			# delivery so the prefetch slot frees (#105).
			_log.error(f"DLX publish timed out after {self.dlx_publish_timeout}s; rejecting | message_id={msg_id} app_id={app_id} queue={queue_name} delivery_tag={message.delivery_tag} exchange={original_exchange} routing_key={original_routing_key}")
			await message.reject(requeue=False)
		except Exception as e:
			_log.error(f"Failed to send message to DLX: {e} | message_id={msg_id} app_id={app_id} delivery_tag={message.delivery_tag} exchange={original_exchange} routing_key={original_routing_key}")
			await message.reject(requeue=False)

	async def _publish_to_dlx(self, dlx_exchange: str, routing_key: str, body: bytes, properties: dict):
		"""Async implementation of DLX publishing.

		Publishes via a dedicated channel with publisher confirms enabled and
		``mandatory=True``, so broker rejection or unroutable destination
		raises (typically ``aio_pika.exceptions.DeliveryError``) instead of
		silently dropping the message. aio-pika's current default for
		``mandatory`` is True, but we pin it explicitly to match the sync path
		and to defend against an upstream default change. The caller is
		responsible for rejecting the original message on failure.
		"""
		message = Message(
			body,
			headers=properties.get('headers'),
			content_type=properties.get('content_type', 'application/json'),
			delivery_mode=properties.get('delivery_mode', 2)
		)

		# aio-pika accepts ``expiration`` as a ``timedelta`` and converts it to
		# AMQP wire-format ms internally. Using timedelta keeps the unit
		# explicit and sidesteps the seconds-vs-ms ambiguity of numeric inputs.
		expiration_ms = properties.get('expiration_ms')
		if expiration_ms is not None:
			message.expiration = timedelta(milliseconds=expiration_ms)

		async def _publish() -> None:
			await self._ensure_dlx_publish_channel()
			exchange = await self._dlx_publish_channel.get_exchange(dlx_exchange)
			await exchange.publish(message, routing_key=routing_key, mandatory=True)

		# Every step can wait forever (no confirm from the broker, stuck channel
		# open); an unbounded wait leaves the delivery unacked and, with a small
		# prefetch, stalls the consumer silently (#105).
		try:
			await asyncio.wait_for(_publish(), timeout=self.dlx_publish_timeout)
		except asyncio.TimeoutError as e:
			# The channel may be what is stuck: drop it so the next DLX publish
			# opens a fresh one instead of waiting the full timeout again.
			await self._drop_dlx_publish_channel()
			raise MrsalDLXPublishTimeout(
				f"DLX publish to {dlx_exchange} did not complete within {self.dlx_publish_timeout}s"
			) from e

	async def _drop_dlx_publish_channel(self) -> None:
		"""Forget the DLX publish channel; close it, bounded, if it is still open."""
		_log = self._logger or log
		channel, self._dlx_publish_channel = self._dlx_publish_channel, None
		if channel is None or channel.is_closed:
			return
		try:
			await asyncio.wait_for(channel.close(), timeout=self.dlx_publish_timeout)
		except Exception:
			_log.debug("DLX publish channel close raised; ignoring.", exc_info=True)
