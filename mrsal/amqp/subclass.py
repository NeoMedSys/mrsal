import asyncio
from mrsal.basemodels import MrsalProtocol
import pika
import json
import logging
import time
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from functools import partial
from mrsal.exceptions import MrsalAbortedSetup, MrsalNoAsyncioLoopError
from logging import WARNING
from pika.exceptions import (
		AMQPConnectionError,
		ChannelClosedByBroker,
		StreamLostError,
		ConnectionClosedByBroker,
		NackError,
		UnroutableError,
		ConnectionWrongStateError,
		)
from aio_pika import connect_robust, Message
from dataclasses import field
from typing import Any, Callable, Literal, Sequence, Type
from tenacity import retry, stop_after_attempt, wait_fixed, wait_exponential, retry_if_exception_type, before_sleep_log
from pydantic import ConfigDict, ValidationError
from pydantic.dataclasses import dataclass

from mrsal.superclass import Mrsal
from mrsal import config

log = logging.getLogger(__name__)

@dataclass
class MrsalBlockingBase(Mrsal):
	"""Shared blocking-connection lifecycle for the sync consumer and publisher.

	Owns the pika ``BlockingConnection`` setup/teardown and the context-manager
	protocol so that the consumer (``MrsalBlockingAMQP``) and the publisher
	(``MrsalBlockingPublisher``) reuse it without one inheriting the other's
	surface.

	:param int blocked_connection_timeout: timeout, in seconds, for the
		connection to remain blocked; if it expires the connection is torn down
		during connection tuning.
	"""
	blocked_connection_timeout: int = 60  # sec

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()
		return False

	def close(self) -> None:
		"""Release channels and the connection.

		Subclasses must implement this; ``__exit__`` relies on it.
		"""
		raise NotImplementedError

	def _ensure_connection(self) -> None:
		"""Idempotent: only connects if not already connected."""
		if self._connection is None or not self._connection.is_open:
			# Close stale connection to avoid leaking TCP sockets
			if self._connection is not None:
				try:
					self._connection.close()
				except Exception:
					pass
			self.setup_blocking_connection()

	def _close_connection(self) -> None:
		"""Close just the underlying connection, swallowing close errors."""
		if self._connection is not None and self._connection.is_open:
			try:
				self._connection.close()
			except Exception:
				log.debug("Connection close raised; ignoring.", exc_info=True)
		self._connection = None

	def setup_blocking_connection(self) -> None:
		"""We can use setup_blocking_connection for establishing a connection to RabbitMQ server specifying connection parameters.
		The connection is blocking which is only advisable to use for the apps with low througput.

		DISCLAIMER: If you expect a lot of traffic to the app or if its realtime then you should use async.

		Parameters
		----------
		context : Dict[str, str]
			context is the structured map with information regarding the SSL options for connecting with rabbit server via TLS.
		"""
		connection_info = f"""
							Mrsal connection parameters:
							host={self.host},
							virtual_host={self.virtual_host},
							port={self.port},
							heartbeat={self.heartbeat},
							ssl={self.ssl}
							"""
		if self.verbose:
			log.info(f"Establishing connection to RabbitMQ on {connection_info}")
		credentials = pika.PlainCredentials(*self.credentials)
		try:
			self._connection = pika.BlockingConnection(
				pika.ConnectionParameters(
					host=self.host,
					port=self.port,
					ssl_options=self.get_ssl_context(async_conn=False),
					virtual_host=self.virtual_host,
					credentials=credentials,
					heartbeat=self.heartbeat,
					blocked_connection_timeout=self.blocked_connection_timeout,
				)
			)

			log.info(f"Boom! Connection established with RabbitMQ on {connection_info}")
		except (AMQPConnectionError, ChannelClosedByBroker, ConnectionClosedByBroker, StreamLostError) as e:
			log.error(f"I tried to connect with the RabbitMQ server but failed with: {e}")
			raise
		except Exception as e:
			log.error(f"Unexpected error caught: {e}")
			raise


@dataclass
class MrsalBlockingAMQP(MrsalBlockingBase):
	"""Blocking RabbitMQ consumer (and legacy per-call publisher).

	For high-throughput publishing prefer ``MrsalBlockingPublisher`` /
	``MrsalBlockingPublisherPool``, which keep the connection and channel warm.
	"""
	_consumer_channel: Any = field(init=False, default=None)
	_dlx_publish_channel: Any = field(init=False, default=None)
	# Declared-state tracking so a reconnect's recovery is explicit and logged
	# rather than only an accidental side effect of tenacity re-running
	# start_consumer. Mirrors MrsalBlockingPublisher._declared_topology /
	# _topology_conn:
	#   _declared_topology -- the full _setup_exchange_and_queue kwargs for the
	#     primary topology (DLX/retry included, since dlx_enable/enable_retry_cycles
	#     are part of it), so the record is faithful enough to re-declare from.
	#     None when nothing was declared here (auto_declare=False).
	#   _active_qos -- the prefetch applied to the consumer channel.
	#   _topology_conn -- pins the tracking to the connection it was declared on;
	#     a reconnect (new connection identity) clears stale state and is logged.
	_declared_topology: dict | None = field(init=False, default=None)
	_active_qos: int | None = field(init=False, default=None)
	_topology_conn: Any = field(init=False, default=None)

	def _reset_declared_state(self) -> None:
		"""Drop all tracked declared state (topology + QoS)."""
		self._declared_topology = None
		self._active_qos = None
		self._topology_conn = None

	def close(self) -> None:
		"""Close channels and connection cleanly.

		Each close is wrapped: a failure on one handle must not leak the next.
		"""
		if self._dlx_publish_channel is not None and self._dlx_publish_channel.is_open:
			try:
				self._dlx_publish_channel.close()
			except Exception:
				log.debug("DLX publish channel close raised; ignoring.", exc_info=True)
		self._dlx_publish_channel = None

		if self._consumer_channel is not None and self._consumer_channel.is_open:
			try:
				self._consumer_channel.close()
			except Exception:
				log.debug("Consumer channel close raised; ignoring.", exc_info=True)
		self._consumer_channel = None

		self._reset_declared_state()
		self._close_connection()

	def _ensure_dlx_publish_channel(self) -> None:
		"""Lazily open a dedicated channel with publisher confirms for DLX writes.

		Why: ``confirm_delivery()`` makes ``basic_publish`` raise on broker
		rejection or unroutable destination (``NackError`` / ``UnroutableError``)
		instead of returning silently. Without it a dropped DLX publish would
		succeed-on-the-wire and the caller would ack the original message,
		causing silent message loss.

		Kept separate from the consumer channel so confirms semantics don't
		affect the consume path. Not safe for concurrent callers; the consume
		loop serializes DLX publishes today.
		"""
		self._ensure_connection()
		if self._dlx_publish_channel is not None and self._dlx_publish_channel.is_open:
			return
		if self._dlx_publish_channel is not None:
			try:
				self._dlx_publish_channel.close()
			except Exception:
				log.debug("Stale DLX publish channel close raised; ignoring.", exc_info=True)
			self._dlx_publish_channel = None
		channel = self._connection.channel()
		channel.confirm_delivery()
		self._dlx_publish_channel = channel

	def _schedule_threadsafe(self, func: Callable, threaded: bool, *args, **kwargs) -> None:
		"""
		Executes an AMQP operation safely based on the threading mode.

		In threaded mode the worker may finish *after* the broker has reset the
		connection (e.g. heartbeat starvation under long callbacks). Calling
		add_callback_threadsafe on a closed BlockingConnection raises
		ConnectionWrongStateError and kills the worker thread. The op (typically
		basic_ack/nack) cannot succeed against a dead connection anyway, so we
		drop it and let the consumer retry pick up the redelivered message.
		"""
		if threaded:
			conn = self._connection
			if conn is None or not conn.is_open:
				log.warning(
					f"Skipping {getattr(func, '__name__', func)!r}: connection is closed; "
					"the broker will redeliver on reconnect."
				)
				return
			cb = partial(func, *args, **kwargs)
			try:
				conn.add_callback_threadsafe(cb)
			except ConnectionWrongStateError as e:
				log.warning(
					f"Could not schedule {getattr(func, '__name__', func)!r} on closed connection: {e}; "
					"the broker will redeliver on reconnect."
				)
		else:
			func(*args, **kwargs)

	@staticmethod
	def _handle_worker_exception(future) -> None:
		"""Callback for ThreadPoolExecutor futures to surface worker exceptions."""
		exc = future.exception()
		if exc is not None:
			log.error(f"Worker thread raised an unhandled exception: {exc}", exc_info=exc)

	def _process_single_message(self, method_frame, properties, body, runtime_config: dict) -> None:
		"""
		Worker method to process a single message. 
		Accepts a config dict to avoid an explosion of arguments.
		"""
		auto_ack = runtime_config['auto_ack']
		threaded = runtime_config['threaded']
		callback = runtime_config['callback']
		callback_args = runtime_config['callback_args']
		payload_model = runtime_config['payload_model']
		dlx_enable = runtime_config['dlx_enable']
		enable_retry_cycles = runtime_config['enable_retry_cycles']
		
		app_id = properties.app_id if hasattr(properties, 'app_id') else 'no AppID'
		msg_id = properties.message_id if hasattr(properties, 'message_id') else 'no MsgID'
		delivery_tag = method_frame.delivery_tag
		
		current_retry = properties.headers.get('x-delivery-count', 0) if properties and properties.headers else 0
		
		if self.verbose:
			log.info(f"Processing message {msg_id} from {app_id} (Retry: {current_retry})")

		should_process = True
		failure_reason: str | None = None
		# When payload_model is set, the validated instance replaces body in the callback.
		callback_body = body
		if payload_model:
			try:
				callback_body = self.validate_payload(payload=body, model=payload_model)
			except (ValidationError, json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
				log.error(f"Payload validation failed for {msg_id}: {e}")
				should_process = False
				failure_reason = f"payload validation: {e!r}"

		if callback and should_process:
			try:
				if callback_args:
					callback(*callback_args, method_frame, properties, callback_body)
				else:
					callback(method_frame, properties, callback_body)
			except Exception as e:
				log.error(f"Callback processing failed for message {msg_id}: {e}")
				should_process = False
				failure_reason = f"callback: {e!r}"

		if not should_process and not auto_ack:
			if dlx_enable and enable_retry_cycles:
				self._schedule_threadsafe(
					self._publish_to_dlx_with_retry_cycle, threaded,
					method_frame, properties, body, failure_reason or "Callback failed",
					runtime_config['exchange_name'], runtime_config['routing_key'],
					enable_retry_cycles, runtime_config['retry_cycle_interval'],
					runtime_config['max_retry_time_limit'], runtime_config['dlx_exchange_name'],
					runtime_config['dlx_routing_key'],
					runtime_config['retry_backoff'], runtime_config['retry_backoff_max'],
				)
			elif dlx_enable:
				log.warning(f"Message {msg_id} sent to dead letter exchange after {current_retry} retries")
				self._schedule_threadsafe(self._consumer_channel.basic_nack, threaded, delivery_tag=delivery_tag, requeue=False)
			else:
				log.warning(f"No dead letter exchange declared for {runtime_config['queue_name']}, proceeding to drop the message -- reflect on your life choices! byebye")
				if self.verbose:
					log.info(f"Dropped message content: {body}")
				self._schedule_threadsafe(self._consumer_channel.basic_nack, threaded, delivery_tag=delivery_tag, requeue=False)

		elif not auto_ack and should_process:
			log.info(f'Message ({msg_id}) from {app_id} received and properly processed -- now dance the funky chicken')
			self._schedule_threadsafe(self._consumer_channel.basic_ack, threaded, delivery_tag=delivery_tag)

	@retry(
		retry=retry_if_exception_type((
			AMQPConnectionError,
			ChannelClosedByBroker,
			ConnectionClosedByBroker,
			StreamLostError,
			)),
		wait=wait_exponential(multiplier=1, min=2, max=60),
		before_sleep=before_sleep_log(log, WARNING)
		)
	def start_consumer(
			self,
			queue_name: str,
			callback: Callable | None = None,
			callback_args: Sequence[str | int | float | bool] | None = None,
			auto_ack: bool = False,
			inactivity_timeout: int | None = None,
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
			threaded: bool = False,
			max_workers: int | None = None
	) -> None:
		"""
		Start the consumer using blocking setup.
		:param str queue_name: The queue to consume from
		:param Callable callback: Invoked as ``callback(*callback_args, method_frame, properties, body)``.
			When ``payload_model`` is set, ``body`` is the validated model instance, not raw bytes.
		:param Sequence callback_args: Optional positional arguments to pass to the callback
		:param bool auto_ack: If True, the broker acks at delivery and the consumer opts out of
			reliability features. Rejected at setup when combined with ``dlx_enable=True`` (DLX is
			unreachable once the broker has acked) or ``threaded=True`` (the executor's submit
			queue is unbounded, so a slow callback grows pending tasks until OOM). Default False.
		:param int inactivity_timeout: Timeout for inactivity in the consumer loop
		:param bool auto_declare: If True, will declare exchange/queue before consuming
		:param bool passive: If True, only check if exchange/queue exists (False for consumers)
		:param str exchange_name: Exchange name for auto_declare
		:param str exchange_type: Exchange type for auto_declare
		:param str routing_key: Routing key for auto_declare
		:param Type payload_model: Pydantic model for payload validation. When set, validation
			failures route to DLX before ``callback`` runs, and the callback receives the
			validated model instance in place of the raw body.
		:param bool dlx_enable: Enable dead letter exchange
		:param str dlx_exchange_name: Custom DLX exchange name
		:param str dlx_routing_key: Custom DLX routing key
		:param bool use_quorum_queues: Use quorum queues for durability
		:param bool enable_retry_cycles: Enable DLX retry cycles
		:param int retry_cycle_interval: Minutes between retry cycles. In ``"exponential"``
			mode this is the base for ``base * 2**cycle`` growth. Default:
			``config.DEFAULT_RETRY_CYCLE_INTERVAL_MIN`` (10).
		:param int max_retry_time_limit: Minutes of cumulative cycle time before the
			message is parked in the terminal ``.dlx`` queue. Default:
			``config.DEFAULT_MAX_RETRY_TIME_LIMIT_MIN`` (480, i.e. 8h — raised from 60
			in 3.9.0 to give exponential delays room to actually retry).
		:param str retry_backoff: "fixed" (flat queue-TTL interval) or "exponential"
			(per-message ``base * 2**cycle`` clamped at ``retry_backoff_max`` with ±20% jitter).
			Default: ``config.DEFAULT_RETRY_BACKOFF`` ("exponential"). Switching modes on a
			running deployment requires deleting the existing ``<queue>.retry`` queue
			(the ``x-message-ttl`` arg becomes inequivalent).
		:param int retry_backoff_max: Per-cycle ceiling in minutes for exponential mode.
			Must be >= ``retry_cycle_interval``. Ignored when ``retry_backoff="fixed"``.
			Default: ``config.DEFAULT_RETRY_BACKOFF_MAX_MIN`` (60).
		:param int max_queue_length: Maximum number of messages in queue
		:param int max_queue_length_bytes: Maximum queue size in bytes
		:param str queue_overflow: "drop-head" or "reject-publish"
		:param bool single_active_consumer: Only one consumer processes at a time
		:param bool lazy_queue: Store messages on disk to save memory
		:param int max_workers: Maximum number of threads in the pool when threaded=True. Defaults to prefetch_count.
		"""
		runtime_config = self._prepare_consumer(
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
			threaded=threaded,
		)
		self._run_consume_loop(
			queue_name=queue_name,
			auto_ack=auto_ack,
			inactivity_timeout=inactivity_timeout,
			runtime_config=runtime_config,
			threaded=threaded,
			max_workers=max_workers,
		)

	def _prepare_consumer(
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
			threaded: bool,
	) -> dict:
		"""Validate config, open the consumer channel, declare topology, and
		return the per-message ``runtime_config``.

		Split out of ``start_consumer`` so the setup and the blocking consume
		loop can be driven separately: the in-memory test broker
		(``mrsal.testing``) calls this to register a consumer and then delivers
		messages by hand, without entering ``_run_consume_loop``. Behaviour for
		the normal ``start_consumer`` path is unchanged.
		"""
		if auto_ack and dlx_enable:
			raise MrsalAbortedSetup(
				'auto_ack=True is incompatible with dlx_enable=True: once the broker has acked '
				'on delivery, failed messages cannot be routed to the DLX. Set dlx_enable=False '
				'to opt out of DLX, or auto_ack=False to keep DLX accountability.'
			)
		if auto_ack and threaded:
			raise MrsalAbortedSetup(
				'auto_ack=True is incompatible with threaded=True: the executor submit queue is '
				'unbounded, so a slow callback grows pending tasks until OOM. Set auto_ack=False '
				'so prefetch_count provides backpressure, or run without threaded=True.'
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

		self._ensure_connection()
		# A reconnect hands back a fresh connection object; tracking pinned to the
		# old one is stale, so drop it before re-recording against this connection.
		# _topology_conn is None only on the first connect, so a non-None mismatch
		# is a genuine reconnect (driven by tenacity re-running start_consumer).
		reconnected = self._topology_conn is not None and self._connection is not self._topology_conn
		if self._connection is not self._topology_conn:
			self._reset_declared_state()
			self._topology_conn = self._connection
		self._consumer_channel = self._connection.channel()
		self._consumer_channel.basic_qos(prefetch_count=self.prefetch_count)
		self._active_qos = self.prefetch_count

		if auto_declare:
			if None in (exchange_name, queue_name, exchange_type, routing_key):
				raise TypeError('Make sure that you are passing in all the necessary args for auto_declare')

			# Hold the full declaration kwargs so the recorded topology is faithful
			# enough to re-declare from (DLX/retry included, via dlx_enable /
			# enable_retry_cycles), rather than a lossy name-only snapshot.
			topology_kwargs = dict(
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
			self._setup_exchange_and_queue(**topology_kwargs, channel=self._consumer_channel)

			if not self.auto_declare_ok:
				raise MrsalAbortedSetup('Auto declaration failed')

			self._declared_topology = topology_kwargs

		if reconnected:
			log.info(
				"Reconnect recovery for consumer on queue %r: re-applied QoS "
				"(prefetch=%s) and %s.",
				queue_name, self._active_qos,
				"re-declared topology" if self._declared_topology is not None
				else "left operator-declared topology in place (auto_declare=False)",
			)

		runtime_config = {
			'callback': callback,
			'callback_args': callback_args,
			'auto_ack': auto_ack,
			'payload_model': payload_model,
			'threaded': threaded,
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
		return runtime_config

	def _run_consume_loop(
			self,
			*,
			queue_name: str,
			auto_ack: bool,
			inactivity_timeout: int | None,
			runtime_config: dict,
			threaded: bool,
			max_workers: int | None,
	) -> None:
		"""Drive the blocking ``consume`` loop using a prepared ``runtime_config``."""
		if threaded:
			max_workers = max_workers or self.prefetch_count

		log.info(f"""
				Straight out of the swamps -- consumer boi listening with config:
					auto_ack: {auto_ack}
					threaded: {threaded}
					max_workers: {max_workers}
					DLX: {runtime_config['dlx_enable']}
					retry cycles: {runtime_config['enable_retry_cycles']}
					retry interval: {runtime_config['retry_cycle_interval']}
					max retry time: {runtime_config['max_retry_time_limit']}
					retry backoff: {runtime_config['retry_backoff']} (cap: {runtime_config['retry_backoff_max']}m)
					DLX name: {runtime_config['dlx_exchange_name']}
				""")

		executor = ThreadPoolExecutor(max_workers=max_workers) if threaded else None

		try:
			for method_frame, properties, body in self._consumer_channel.consume(
							queue=queue_name, auto_ack=auto_ack, inactivity_timeout=inactivity_timeout):

				if method_frame:
					if threaded:
						log.info("Threaded processes started to ensure heartbeat during long processes -- sauber!")
						future = executor.submit(self._process_single_message, method_frame, properties, body, runtime_config)
						future.add_done_callback(self._handle_worker_exception)
					else:
						self._process_single_message(method_frame, properties, body, runtime_config)
		except (AMQPConnectionError, ConnectionClosedByBroker, StreamLostError) as e:
			log.error(f"Ooooooopsie! I caught a connection error while consuming messaiges: {e}")
			raise
		except Exception as e:
			log.error(f'Oh lordy lord! I failed consuming ze messaj with: {e}')
			raise
		finally:
			if executor is not None:
				executor.shutdown(wait=True, cancel_futures=True)

	@retry(
		retry=retry_if_exception_type((
			NackError,
			UnroutableError,
			AMQPConnectionError,
			ChannelClosedByBroker,
			ConnectionClosedByBroker,
			StreamLostError
			)),
		stop=stop_after_attempt(3),
		wait=wait_fixed(2),
		before_sleep=before_sleep_log(log, WARNING)
		)
	def publish_message(
		self,
		exchange_name: str,
		routing_key: str,
		message: str | bytes | None,
		exchange_type: str,
		queue_name: str,
		auto_declare: bool = True,
		passive: bool = True,
		prop: pika.BasicProperties | None = None,
	) -> None:
		"""Publish message to the exchange specifying routing key and properties.

		:param str exchange: The exchange to publish to
		:param str routing_key: The routing key to bind on
		:param bytes body: The message body; empty string if no body
		:param pika.spec.BasicProperties properties: message properties
		:param bool fast_setup:
				- when True, will the method create the specified exchange, queue and bind them together using the routing kye.
				- If False, this method will check if the specified exchange and queue already exist before publishing.

		:raises UnroutableError: raised when a message published in publisher-acknowledgments mode (see `BlockingChannel.confirm_delivery`) is returned via `Basic.Return` followed by `Basic.Ack`.
		:raises NackError: raised when a message published in publisher-acknowledgements mode is Nack'ed by the broker. See `BlockingChannel.confirm_delivery`.
		"""

		self._validate_message_body(message)
		# connect and use only blocking
		self._ensure_connection()
		ch = self._connection.channel()
		# Required for the NackError/UnroutableError retry on this method to actually fire;
		# without confirms, basic_publish is fire-and-forget.
		ch.confirm_delivery()

		try:
			if auto_declare:
				if None in (exchange_name, queue_name, exchange_type, routing_key):
					raise TypeError('Make sure that you are passing in all the necessary args for auto_declare')

				self._setup_exchange_and_queue(
					exchange_name=exchange_name,
					queue_name=queue_name,
					exchange_type=exchange_type,
					routing_key=routing_key,
					passive=passive,
					channel=ch
					)
			try:
				# Publish the message by serializing it in json dump
				# NOTE! we are not dumping a json anymore here! This allows for more flexibility
				ch.basic_publish(exchange=exchange_name, routing_key=routing_key, body=message, properties=prop)
				log.info(f"Message published to exchange {exchange_name} with routing key {routing_key}")

			except UnroutableError as e:
				log.error(f"Producer could not publish message:{message!r} to the exchange {exchange_name} with a routing key {routing_key}: {e}", exc_info=True)
				raise
			except NackError as e:
				log.error(f"Message NACKed by broker: {e}")
				raise
			except Exception as e:
				log.error(f"Unexpected error while publishing message: {e}")
				raise
		finally:
			try:
				ch.close()
			except Exception:
				pass

	@retry(
		retry=retry_if_exception_type((
			NackError,
			UnroutableError,
			AMQPConnectionError,
			ChannelClosedByBroker,
			ConnectionClosedByBroker,
			StreamLostError
			)),
		stop=stop_after_attempt(3),
		wait=wait_fixed(2),
		before_sleep=before_sleep_log(log, WARNING)
		)
	def publish_messages(
		self,
		mrsal_protocol_collection: dict[str, dict[str, str | bytes]],
		prop: pika.BasicProperties | None = None,
		auto_declare: bool = True,
		passive: bool = True
	) -> None:
		"""Publish message to the exchange specifying routing key and properties.

		mrsal_protocol_collection :  dict[str, dict[str, str | bytes]]
			This is a collection of the protcols needed for publishing to multiple exhanges at once

			expected collection: {
				inbound_app_1: {message: bytes | str, routing_key: str, queue_name: str, exchange_type: str, exchange_name: str},
				inbound_app_2: {message: bytes | str, routing_key: str, queue_name: str, exchange_type: str, exchange_name: str},
				.,
				.
			}

		:raises UnroutableError: raised when a message published in publisher-acknowledgments mode (see `BlockingChannel.confirm_delivery`) is returned via `Basic.Return` followed by `Basic.Ack`.
		:raises NackError: raised when a message published in publisher-acknowledgements mode is Nack'ed by the broker. See `BlockingChannel.confirm_delivery`.
		"""

		self._ensure_connection()
		ch = self._connection.channel()
		# Required for the NackError/UnroutableError retry on this method to actually fire;
		# without confirms, basic_publish is fire-and-forget.
		ch.confirm_delivery()

		try:
			for inbound_app_id, mrsal_protocol in mrsal_protocol_collection.items():
				protocol = MrsalProtocol(**mrsal_protocol)

				self._validate_message_body(protocol.message)

				if auto_declare:
					self._setup_exchange_and_queue(
						exchange_name=protocol.exchange_name,
						queue_name=protocol.queue_name,
						exchange_type=protocol.exchange_type,
						routing_key=protocol.routing_key,
						passive=passive,
						channel=ch
						)
				try:
					# Publish the message by serializing it in json dump
					# NOTE! we are not dumping a json anymore here! This allows for more flexibility
					ch.basic_publish(
							exchange=protocol.exchange_name,
							routing_key=protocol.routing_key,
							body=protocol.message,
							properties=prop
							)
					log.info(f"Message for inbound app {inbound_app_id} published to exchange {protocol.exchange_name} with routing key {protocol.routing_key}")

				except UnroutableError as e:
					log.error(f"Producer could not publish message:{protocol.message!r} to the exchange {protocol.exchange_name} with a routing key {protocol.routing_key}: {e}", exc_info=True)
					raise
				except NackError as e:
					log.error(f"Message NACKed by broker: {e}")
					raise
				except Exception as e:
					log.error(f"Unexpected error while publishing message: {e}")
					raise
		finally:
			try:
				ch.close()
			except Exception:
				pass

	def _publish_to_dlx_with_retry_cycle(
			self,
			method_frame, properties, body, processing_error: str,
			original_exchange: str, original_routing_key: str,
			enable_retry_cycles: bool, retry_cycle_interval: int,
			max_retry_time_limit: int, dlx_exchange_name: str | None,
			dlx_routing_key: str | None = None,
			retry_backoff: Literal["fixed", "exponential"] = config.DEFAULT_RETRY_BACKOFF,
			retry_backoff_max: int = config.DEFAULT_RETRY_BACKOFF_MAX_MIN):
		"""Publish message to DLX with retry cycle headers.

		At-least-once delivery for DLX: the publish uses ``confirm_delivery()``
		on a dedicated channel, so broker rejection or connection loss raises
		and the original message is nacked (not acked). If the process crashes
		between the confirmed DLX publish and the original ack, the message
		will be redelivered and re-published to DLX. Consumers must be idempotent.
		"""
		try:
			# Use common logic from superclass
			self._handle_dlx_with_retry_cycle_sync(
				method_frame=method_frame,
				properties=properties,
				body=body,
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
			)
			
			# Acknowledge original message
			self._consumer_channel.basic_ack(delivery_tag=method_frame.delivery_tag)

		except Exception as e:
			msg_id = properties.message_id if hasattr(properties, 'message_id') else 'unknown'
			app_id = properties.app_id if hasattr(properties, 'app_id') else 'unknown'
			log.error(f"Failed to send message to DLX: {e} | message_id={msg_id} app_id={app_id} delivery_tag={method_frame.delivery_tag} exchange={original_exchange} routing_key={original_routing_key}")
			self._consumer_channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)

	def _publish_to_dlx(self, dlx_exchange: str, routing_key: str, body: bytes, properties: dict):
		"""Blocking implementation of DLX publishing.

		Publishes via a dedicated channel with ``confirm_delivery()`` enabled
		and ``mandatory=True``, so broker rejection or unroutable destination
		raises (``pika.exceptions.NackError`` / ``UnroutableError``) instead of
		silently dropping the message. Without ``mandatory=True`` the broker
		ack-and-discards unroutable publishes under publisher confirms; this
		would silently lose messages if the ``.retry`` or ``.dlx`` queue setup
		failed. The caller is responsible for nacking the original message on
		failure.
		"""
		# AMQP wire format for `expiration` is a string of milliseconds; pika
		# forwards `BasicProperties.expiration` verbatim, so format here.
		expiration_ms = properties.get('expiration_ms')
		pika_properties = pika.BasicProperties(
			headers=properties.get('headers'),
			delivery_mode=properties.get('delivery_mode', 2),
			content_type=properties.get('content_type', 'application/json'),
			expiration=str(expiration_ms) if expiration_ms is not None else None
		)

		self._ensure_dlx_publish_channel()
		self._dlx_publish_channel.basic_publish(
			exchange=dlx_exchange,
			routing_key=routing_key,
			body=body,
			properties=pika_properties,
			mandatory=True,
		)


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class MrsalAsyncAMQP(Mrsal):
	"""Handles asynchronous connection with RabbitMQ using aio-pika."""

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
		if self._stop_event is not None:
			self._stop_event.set()
		if self._consumer_iterator is not None:
			try:
				await self._consumer_iterator.close()
			except Exception:
				log.debug("Consumer iterator close raised; ignoring.", exc_info=True)

	async def close(self) -> None:
		"""Close channels and connection cleanly.

		Each close is wrapped: a failure on one handle must not leak the next.
		"""
		if self._dlx_publish_channel is not None and not self._dlx_publish_channel.is_closed:
			try:
				await self._dlx_publish_channel.close()
			except Exception:
				log.debug("DLX publish channel close raised; ignoring.", exc_info=True)
		self._dlx_publish_channel = None

		if self._channel is not None and not self._channel.is_closed:
			try:
				await self._channel.close()
			except Exception:
				log.debug("Consumer channel close raised; ignoring.", exc_info=True)
		self._channel = None

		if self._connection is not None and not self._connection.is_closed:
			try:
				await self._connection.close()
			except Exception:
				log.debug("Connection close raised; ignoring.", exc_info=True)
		self._connection = None

	async def __aenter__(self):
		return self

	async def __aexit__(self, exc_type, exc_val, exc_tb):
		await self.close()
		return False

	async def _ensure_async_connection(self) -> None:
		"""Idempotent: only connects if not already connected. Closes stale connections."""
		if self._connection is None or self._connection.is_closed:
			if self._connection is not None:
				try:
					await self._connection.close()
				except Exception:
					log.debug("Stale connection close raised; ignoring.", exc_info=True)
				self._connection = None
			await self.setup_async_connection()

	async def _ensure_consumer_channel(self) -> None:
		"""Close any prior open channel before opening a new one (prevents leak on tenacity retry).

		Not safe for concurrent callers; start_consumer is the only call site.
		"""
		if self._channel is not None and not self._channel.is_closed:
			try:
				await self._channel.close()
			except Exception:
				log.debug("Stale channel close raised; ignoring.", exc_info=True)
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
		await self._ensure_async_connection()
		if self._dlx_publish_channel is not None and not self._dlx_publish_channel.is_closed:
			return
		if self._dlx_publish_channel is not None:
			try:
				await self._dlx_publish_channel.close()
			except Exception:
				log.debug("Stale DLX publish channel close raised; ignoring.", exc_info=True)
			self._dlx_publish_channel = None
		self._dlx_publish_channel = await self._connection.channel(publisher_confirms=True)

	async def setup_async_connection(self):
		"""Setup an asynchronous connection to RabbitMQ using aio-pika."""
		log.info(f"Establishing async connection to RabbitMQ on {self.host}:{self.port}")
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
			log.info("Async connection established successfully.")
		except (AMQPConnectionError, StreamLostError, ChannelClosedByBroker, ConnectionClosedByBroker) as e:
			log.error(f"Error establishing async connection: {e}", exc_info=True)
			raise
		except Exception as e:
			log.error(f'Oh my lordy lord! I caugth an unexpected exception while trying to connect: {e}', exc_info=True)
			raise

	async def _handle_message(self, message, runtime_config: dict) -> None:
		"""Process a single message: validate -> callback -> ack/DLX.

		Shared by the sequential and concurrent paths in ``start_consumer`` so
		the failure/ack policy stays in one place regardless of dispatch mode.
		"""
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
			log.info(f"""
						Message received with:
						- Redelivery: {message.redelivered}
						- Exchange: {message.exchange}
						- Routing Key: {message.routing_key}
						- Delivery Tag: {message.delivery_tag}
						- Auto Ack: {auto_ack}
						""")

		current_retry = message.headers.get('x-delivery-count', 0) if message.headers else 0
		should_process = True
		failure_reason: str | None = None
		# When payload_model is set, the validated instance replaces message.body in the callback.
		callback_body = message.body

		if payload_model:
			try:
				callback_body = self.validate_payload(payload=message.body, model=payload_model)
			except (ValidationError, json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
				log.error(f"Payload validation failed: {e}", exc_info=True)
				should_process = False
				failure_reason = f"payload validation: {e!r}"

		if callback and should_process:
			try:
				if callback_args:
					await callback(*callback_args, message, properties, callback_body)
				else:
					await callback(message, properties, callback_body)
			except Exception as e:
				log.error(f"Splæt! Error processing message with callback: {e}", exc_info=True)
				should_process = False
				failure_reason = f"callback: {e!r}"

		# auto_ack=True: broker already acked; skip DLX (caller opted out of accountability)
		if auto_ack:
			if not should_process:
				log.warning(
					f"Message {msg_id} dropped (auto_ack=True): {failure_reason} | "
					f"app_id={app_id} routing_key={message.routing_key}"
				)
			return

		if not should_process:
			if dlx_enable and enable_retry_cycles:
				await self._async_publish_to_dlx_with_retry_cycle(
					message, properties, failure_reason or "Callback processing failed",
					exchange_name, routing_key, enable_retry_cycles,
					retry_cycle_interval, max_retry_time_limit, dlx_exchange_name,
					dlx_routing_key,
					retry_backoff, retry_backoff_max,
				)
			elif dlx_enable:
				await message.reject(requeue=False)
				log.warning(f"Message {msg_id} sent to dead letter exchange after {current_retry} retries")
			else:
				await message.reject(requeue=False)
				log.warning(f"No dead letter exchange for {queue_name} declared, proceeding to drop the message -- Ponder you life choices! byebye")
				if self.verbose:
					log.info(f"Dropped message content: {message.body}")
			return

		await message.ack()
		log.info(f'Young grasshopper! Message ({msg_id}) from {app_id} received and properly processed.')

	async def _handle_message_with_release(self, message, runtime_config: dict,
										semaphore: asyncio.Semaphore) -> None:
		"""Task body for concurrent dispatch: handle one message and release the slot.

		Wrapped so a crash inside ``_handle_message`` cannot leak the semaphore
		permit or kill the parent loop. Errors are logged; the iterator keeps
		moving.
		"""
		try:
			await self._handle_message(message, runtime_config)
		except Exception:
			log.exception("Unhandled error processing message in concurrent task")
		finally:
			semaphore.release()

	@retry(
		retry=retry_if_exception_type((
			AMQPConnectionError,
			ChannelClosedByBroker,
			ConnectionClosedByBroker,
			StreamLostError,
			)),
		wait=wait_exponential(multiplier=1, min=2, max=60),
		before_sleep=before_sleep_log(log, WARNING)
		)
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
		await self._run_consume_loop_async(
			queue=queue,
			runtime_config=runtime_config,
			auto_ack=auto_ack,
			max_concurrent_tasks=max_concurrent_tasks,
			drain_timeout=drain_timeout,
		)

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
				await self.close()
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

		log.info(f"Straight out of the swamps -- consumer boi listening with config: {consumer_config}")

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
		# Lifecycle primitives are created here (not in __init__) because asyncio.Event
		# needs to bind to a running loop; start_consumer is the first point we have one.
		# Lazy creation also preserves a stop() that arrived during tenacity exponential
		# backoff: if the previous attempt set the event and is being retried, we keep
		# the set state instead of clobbering it with a fresh unset Event.
		if self._stop_event is None:
			self._stop_event = asyncio.Event()
		if self._inflight_tasks is None:
			self._inflight_tasks = set()
		if max_concurrent_tasks is not None and max_concurrent_tasks > 0:
			semaphore = asyncio.Semaphore(max_concurrent_tasks)
		else:
			semaphore = None

		try:
			# async with: ensures the consumer cancellation is deterministically delivered
			# to the broker on exception or generator GC. Without it, channel state can
			# be left mid-cancel.
			async with queue.iterator(no_ack=auto_ack) as it:
				self._consumer_iterator = it
				async for message in it:
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
						# add_done_callback invokes the callback with the task as its
						# single argument; set.discard takes one argument and removes
						# it from the set, so the signatures line up. This keeps the
						# in-flight set bounded without an explicit wrapper coroutine.
						task.add_done_callback(self._inflight_tasks.discard)
		finally:
			self._consumer_iterator = None
			if self._inflight_tasks:
				# Drain in-flight messages before returning so callers observing
				# start_consumer() returning can trust that no work is still pending.
				# gather() swallows individual task exceptions (already logged inside
				# _handle_message_with_release).
				pending = list(self._inflight_tasks)
				log.info(f"Draining {len(pending)} in-flight message task(s) before exit")
				drain_coro = asyncio.gather(*pending, return_exceptions=True)
				if drain_timeout is None:
					await drain_coro
				else:
					try:
						await asyncio.wait_for(drain_coro, timeout=drain_timeout)
					except asyncio.TimeoutError:
						still_pending = [t for t in pending if not t.done()]
						log.warning(
							f"Drain timeout after {drain_timeout}s: cancelling "
							f"{len(still_pending)} unfinished task(s); their messages "
							f"will be redelivered by the broker."
						)
						for task in still_pending:
							task.cancel()
						await asyncio.gather(*still_pending, return_exceptions=True)

	async def _async_publish_to_dlx_with_retry_cycle(self, message, properties, processing_error: str,
												original_exchange: str, original_routing_key: str,
												enable_retry_cycles: bool, retry_cycle_interval: int,
												max_retry_time_limit: int, dlx_exchange_name: str | None,
												dlx_routing_key: str | None = None,
												retry_backoff: Literal["fixed", "exponential"] = config.DEFAULT_RETRY_BACKOFF,
												retry_backoff_max: int = config.DEFAULT_RETRY_BACKOFF_MAX_MIN):
		"""Async publish message to DLX with retry cycle headers.

		At-least-once delivery for DLX: the publish uses publisher confirms on
		a dedicated channel, so broker rejection or connection loss raises and
		the original message is rejected (not acked). If the process crashes
		between the confirmed DLX publish and the original ack, the message
		will be redelivered and re-published to DLX. Consumers must be idempotent.
		"""
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
			)
			
			# Acknowledge original message
			await message.ack()
			
		except Exception as e:
			msg_id = properties.message_id if hasattr(properties, 'message_id') else 'unknown'
			app_id = properties.app_id if hasattr(properties, 'app_id') else 'unknown'
			log.error(f"Failed to send message to DLX: {e} | message_id={msg_id} app_id={app_id} delivery_tag={message.delivery_tag} exchange={original_exchange} routing_key={original_routing_key}")
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

		await self._ensure_dlx_publish_channel()
		exchange = await self._dlx_publish_channel.get_exchange(dlx_exchange)
		await exchange.publish(message, routing_key=routing_key, mandatory=True)


# How many times publish() retries across a dropped connection/channel before
# giving up, and how long it waits between attempts. Mirrors the tenacity
# policy on MrsalBlockingAMQP.publish_message (3 attempts, fixed 2s).
_PUBLISH_ATTEMPTS = 3
_PUBLISH_RETRY_WAIT_SEC = 2

# Broker rejected the message under publisher confirms (unroutable / nacked):
# terminal, retrying won't change the outcome.
_TERMINAL_PUBLISH_ERRORS = (UnroutableError, NackError)
# Connection or channel died mid-publish: reconnect and retry.
_RETRIABLE_PUBLISH_ERRORS = (
	AMQPConnectionError,
	ChannelClosedByBroker,
	ConnectionClosedByBroker,
	StreamLostError,
	ConnectionWrongStateError,
)


@dataclass
class MrsalBlockingPublisher(MrsalBlockingBase):
	"""Long-lived blocking publisher: one reused connection + channel.

	Two efficiencies over ``MrsalBlockingAMQP.publish_message`` -- which opens
	and closes a channel on every call (and whose callers typically open and
	close a whole connection per publish):

	  * channel reuse -- a single confirm-enabled channel is kept open across
	    publishes instead of being opened and closed each time.
	  * topology cache -- the passive ``Exchange.Declare`` / ``Queue.Declare``
	    round-trips run only the first time a given target is published to on a
	    connection, not on every publish.

	Publishes are sent with ``mandatory=True`` under publisher confirms, so an
	unroutable target raises ``UnroutableError`` rather than being silently
	dropped.

	NOT thread-safe: pika's BlockingConnection is owned by a single thread. Use
	one instance per thread, or let ``MrsalBlockingPublisherPool`` hand out one
	instance per concurrent caller.
	"""
	_publish_channel: Any = field(init=False, default=None)
	_declared_topology: Any = field(init=False, default_factory=set)
	_topology_conn: Any = field(init=False, default=None)

	def _ensure_publish_channel(self) -> None:
		"""Open (or reopen) the persistent confirm-enabled publish channel.

		Reconnects the underlying connection if it dropped. When a *new*
		connection is established the topology cache is cleared, because the
		freshly opened connection has verified nothing yet.
		"""
		self._ensure_connection()
		if self._connection is not self._topology_conn:
			# Reconnected (or first connect): nothing verified on this socket yet.
			self._declared_topology.clear()
			self._topology_conn = self._connection
			self._publish_channel = None
		if self._publish_channel is not None and self._publish_channel.is_open:
			return
		channel = self._connection.channel()
		# Confirms make basic_publish raise NackError/UnroutableError instead of
		# silently dropping -- callers rely on that signal.
		channel.confirm_delivery()
		self._publish_channel = channel

	def _reset_publish_channel(self) -> None:
		"""Drop the channel so the next publish reopens it."""
		if self._publish_channel is not None:
			try:
				if self._publish_channel.is_open:
					self._publish_channel.close()
			except Exception:
				log.debug("Publish channel close raised during reset; ignoring.", exc_info=True)
		self._publish_channel = None

	def publish(
		self,
		exchange_name: str,
		routing_key: str,
		message: str | bytes,
		exchange_type: str,
		queue_name: str,
		auto_declare: bool = True,
		passive: bool = True,
		prop: pika.BasicProperties | None = None,
	) -> None:
		"""Publish on the reused channel, declaring topology at most once per target.

		Same arguments as ``MrsalBlockingAMQP.publish_message``. Sent with
		``mandatory=True`` under publisher confirms, so an unroutable target
		raises ``UnroutableError`` rather than being silently dropped. On a
		dropped connection/channel the publish is retried after reconnecting;
		terminal broker rejections (``NackError`` / ``UnroutableError``) and a
		failed topology declaration are raised so the caller decides what to do.
		"""
		self._validate_message_body(message)

		topology_key = (exchange_name, exchange_type, queue_name, routing_key, passive)
		last_exc: Exception | None = None
		for attempt in range(1, _PUBLISH_ATTEMPTS + 1):
			try:
				self._ensure_publish_channel()

				if auto_declare and topology_key not in self._declared_topology:
					if None in (exchange_name, queue_name, exchange_type, routing_key):
						raise TypeError('Make sure that you are passing in all the necessary args for auto_declare')
					self._setup_exchange_and_queue(
						exchange_name=exchange_name,
						queue_name=queue_name,
						exchange_type=exchange_type,
						routing_key=routing_key,
						passive=passive,
						channel=self._publish_channel,
					)
					# auto_declare_ok is the success flag for both passive checks
					# and active declares; _setup_exchange_and_queue swallows the
					# broker error, so guard on it (and only cache on success) for
					# both modes -- a failed passive 404 also closed the channel.
					if not self.auto_declare_ok:
						self._reset_publish_channel()
						raise MrsalAbortedSetup(
							f"Topology declaration failed for exchange {exchange_name} / queue {queue_name}; refusing to publish."
						)
					self._declared_topology.add(topology_key)

				self._publish_channel.basic_publish(
					exchange=exchange_name, routing_key=routing_key, body=message,
					properties=prop, mandatory=True,
				)
				if self.verbose:
					log.info(f"Message published to exchange {exchange_name} with routing key {routing_key}")
				return

			except _TERMINAL_PUBLISH_ERRORS as e:
				# Confirm-mode rejection: terminal, retrying won't help. The
				# channel stays usable for the next caller.
				log.error(f"Broker rejected publish to {exchange_name}/{routing_key}: {e}")
				raise
			except _RETRIABLE_PUBLISH_ERRORS as e:
				# Connection or channel died. Drop the channel and reconnect on
				# the next attempt. The topology cache is left intact: declared
				# exchanges/queues are broker-side state that outlives the
				# channel, and a genuine reconnect clears the cache anyway via
				# _ensure_publish_channel.
				last_exc = e
				self._reset_publish_channel()
				log.warning(f"Publish attempt {attempt}/{_PUBLISH_ATTEMPTS} to {exchange_name}/{routing_key} failed: {e}")
				if attempt < _PUBLISH_ATTEMPTS:
					time.sleep(_PUBLISH_RETRY_WAIT_SEC)
		assert last_exc is not None
		raise last_exc

	def close(self) -> None:
		"""Close the publish channel, then the connection."""
		self._reset_publish_channel()
		self._declared_topology.clear()
		self._topology_conn = None
		self._close_connection()


class MrsalBlockingPublisherPool:
	"""Thread-safe pool of warm ``MrsalBlockingPublisher`` connections.

	Each publisher is checked out to exactly one thread at a time, which is the
	safe way to share pika's non-thread-safe BlockingConnection across a thread
	pool (e.g. FastAPI's sync-route worker threads). Connections are kept warm
	between checkouts, removing the per-publish TLS/AMQP handshake.

	Revalidation is lazy: ``MrsalBlockingPublisher.publish`` reconnects on use,
	so a connection that dropped while idle is transparently reopened on its
	next checkout.
	"""

	def __init__(self, size: int = 4, **publisher_kwargs: Any) -> None:
		if size < 1:
			raise ValueError("Pool size must be >= 1")
		self._size = size
		self._publisher_kwargs = publisher_kwargs
		self._idle: queue.Queue = queue.Queue(maxsize=size)
		self._lock = threading.Lock()
		self._created = 0
		self._closed = False

	def _checkout(self, timeout: float | None) -> MrsalBlockingPublisher:
		with self._lock:
			if self._closed:
				raise RuntimeError("Pool is closed")
			# Grow lazily up to size; only create when nothing is idle so warm
			# connections are reused in preference to opening new ones.
			if self._created < self._size and self._idle.empty():
				pub = MrsalBlockingPublisher(**self._publisher_kwargs)
				self._created += 1
				return pub
		return self._idle.get(timeout=timeout)

	def _checkin(self, pub: "MrsalBlockingPublisher") -> None:
		if self._closed:
			try:
				pub.close()
			except Exception:
				log.debug("Publisher close on checkin raised; ignoring.", exc_info=True)
			return
		self._idle.put(pub)

	@contextmanager
	def acquire(self, timeout: float | None = None):
		"""Borrow a publisher for the duration of the ``with`` block.

		When the pool is saturated this blocks for a free connection; with the
		default ``timeout=None`` it blocks indefinitely, so pass a timeout
		(``queue.Empty`` is raised on expiry) if callers must not stall. Do not
		acquire re-entrantly from within an ``acquire`` block on a size-bounded
		pool -- a handler holding one publisher while waiting for another can
		deadlock the pool.
		"""
		pub = self._checkout(timeout)
		try:
			yield pub
		finally:
			self._checkin(pub)

	def close_all(self) -> None:
		"""Stop new checkouts and close idle publishers. Idempotent.

		Only currently-idle publishers are closed here; any checked out by
		another thread are closed when returned (see ``_checkin``), so an
		in-flight publish is never closed out from under it.
		"""
		with self._lock:
			self._closed = True
		while True:
			try:
				pub = self._idle.get_nowait()
			except queue.Empty:
				break
			try:
				pub.close()
			except Exception:
				log.debug("Publisher close raised during close_all; ignoring.", exc_info=True)
