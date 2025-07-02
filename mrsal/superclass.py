 # external
import os
import ssl
import pika
import logging
from datetime import datetime, timezone
from ssl import SSLContext
from typing import Any, Type
from pika.connection import SSLOptions
from aio_pika import ExchangeType as AioExchangeType, Queue as AioQueue, Exchange as AioExchange
from pydantic.dataclasses import dataclass

from pydantic.deprecated.tools import json

# internal
from mrsal import config
from mrsal.exceptions import MrsalAbortedSetup, MrsalSetupError

log = logging.getLogger(__name__)

@dataclass
class Mrsal:
	"""
	Mrsal creates a layer on top of Pika's core, providing methods to setup a RabbitMQ broker with multiple functionalities.

	Properties:
		:param str host: Hostname or IP Address to connect to
		:param int port: TCP port to connect to
		:param pika.credentials.Credentials credentials: auth credentials
		:param str virtual_host: RabbitMQ virtual host to use
		:param bool verbose: If True then more INFO logs will be printed
		:param int heartbeat: Controls RabbitMQ's server heartbeat timeout negotiation
		:param int prefetch_count: Specifies a prefetch window in terms of whole messages.
		:param bool ssl: Set this flag to true if you want to connect externally to the rabbit server.
	"""

	host: str
	port: int
	credentials: tuple[str, str]
	virtual_host: str
	ssl: bool = False
	verbose: bool = False
	prefetch_count: int = 5
	heartbeat: int = 60  # sec
	dlx_enable: bool = True
	dlx_exchange_name = None
	max_retries: int = 3
	use_quorum_queues: bool = True
	_connection = None
	_channel = None

	def __post_init__(self) -> None:
		if self.ssl:
			tls_dict = {
					'crt': os.environ.get('RABBITMQ_CERT'),
					'key': os.environ.get('RABBITMQ_KEY'),
					'ca': os.environ.get('RABBITMQ_CAFILE')
					}
			# empty string handling
			self.tls_dict = {cert: (env_var if env_var != '' else None) for cert, env_var in tls_dict.items()}
			config.ValidateTLS(**self.tls_dict)

	def _setup_exchange_and_queue(self,
								 exchange_name: str, queue_name: str, exchange_type: str,
								 routing_key: str, exch_args: dict[str, str] | None = None,
								 queue_args: dict[str, str] | None = None,
								 bind_args: dict[str, str] | None = None,
								 exch_durable: bool = True, queue_durable: bool =True,
								 passive: bool = False, internal: bool = False,
								 auto_delete: bool = False, exclusive: bool = False,
								 dlx_enable: bool = True, dlx_exchange_name: str | None = None,
								 dlx_routing_key: str | None = None, use_quorum_queues: bool = True
								 ) -> None:


		if dlx_enable:
			dlx_name = dlx_exchange_name or f"{exchange_name}.dlx"
			dlx_routing = dlx_routing_key or routing_key
			try:
				self._declare_exchange(
					exchange=dlx_name,
					exchange_type=exchange_type,
					arguments=None,
					durable=exch_durable,
					passive=False,
					internal=internal,
					auto_delete=auto_delete
				)
				if self.verbose:
					log.info(f"Dead letter exchange {dlx_name} declared successfully")

			except MrsalSetupError as e:
				log.warning(f"DLX {dlx_name} might already exist or failed to create: {e}")

			if queue_args is None:
				queue_args = {}

			queue_args.update({
				'x-dead-letter-exchange': dlx_name,
				'x-dead-letter-routing-key': dlx_routing
			})

		if use_quorum_queues:
			if queue_args is None:
				queue_args = {}

			queue_args.update({
				'x-queue-type': 'quorum',
				'x-quorum-initial-group-size': 3  # Good default for 3+ node clusters
			})

			if self.verbose:
				log.info(f"Queue {queue_name} configured as quorum queue for enhanced reliability")


		declare_exhange_dict = {
				'exchange': exchange_name,
				'exchange_type': exchange_type,
				'arguments': exch_args,
				'durable': exch_durable,
				'passive': passive,
				'internal': internal,
				'auto_delete': auto_delete
				}

		declare_queue_dict = {
				'queue': queue_name,
				'arguments': queue_args,
				'durable': queue_durable,
				'passive': passive,
				'exclusive': exclusive,
				'auto_delete': auto_delete
				}

		declare_queue_binding_dict = {
				'exchange': exchange_name,
				'queue': queue_name,
				'routing_key': routing_key,
				'arguments': bind_args

				}
		try:
			self._declare_exchange(**declare_exhange_dict)
			self._declare_queue(**declare_queue_dict)
			self._declare_queue_binding(**declare_queue_binding_dict)
			self.auto_declare_ok = True
			log.info(f"Exchange {exchange_name} and Queue {queue_name} set up successfully.")
		except MrsalSetupError as e:
			log.error(f'Splæt! I failed the declaration setup with {e}', exc_info=True)
			self.auto_declare_ok = False

	async def _async_setup_exchange_and_queue(self,
											  exchange_name: str, queue_name: str,
											  routing_key: str, exchange_type: str,
											  exch_args: dict[str, str] | None = None,
											  queue_args: dict[str, str] | None = None,
											  bind_args: dict[str, str] | None = None,
											  exch_durable: bool = True, queue_durable: bool = True,
											  passive: bool = False, internal: bool = False,
											  auto_delete: bool = False, exclusive: bool = False,
											  dlx_enable: bool = True,
											  dlx_exchange_name: str | None = None,
											  dlx_routing_key: str | None = None,
											  use_quorum_queues: bool = True
											  ) -> AioQueue | None:
		"""Setup exchange and queue with bindings asynchronously."""
		if not self._connection:
			raise MrsalAbortedSetup("Oh my Oh my! Connection not found when trying to run the setup!")

		if dlx_enable:
			dlx_name = dlx_exchange_name or f"{exchange_name}.dlx"
			dlx_routing = dlx_routing_key or routing_key

			try:
				await self._async_declare_exchange(
					exchange=dlx_name,
					exchange_type=exchange_type,
					arguments=None,
					durable=exch_durable,
					passive=False,
					internal=internal,
					auto_delete=auto_delete
				)

				if self.verbose:
					log.info(f"Dead letter exchange {dlx_name} declared successfully")

			except MrsalSetupError as e:
				log.warning(f"DLX {dlx_name} might already exist or failed to create: {e}")

			if queue_args is None:
				queue_args = {}

			queue_args.update({
				'x-dead-letter-exchange': dlx_name,
				'x-dead-letter-routing-key': dlx_routing
			})

		if use_quorum_queues:
			if queue_args is None:
				queue_args = {}

			queue_args.update({
				'x-queue-type': 'quorum',
				'x-quorum-initial-group-size': 3  # Good default for 3+ node clusters
			})

			if self.verbose:
				log.info(f"Queue {queue_name} configured as quorum queue for enhanced reliability")

		async_declare_exhange_dict = {
				'exchange': exchange_name,
				'exchange_type': exchange_type,
				'arguments': exch_args,
				'durable': exch_durable,
				'passive': passive,
				'internal': internal,
				'auto_delete': auto_delete
				}

		async_declare_queue_dict = {
				'queue_name': queue_name,
				'arguments': queue_args,
				'durable': queue_durable,
				'exclusive': exclusive,
				'auto_delete': auto_delete,
				'passive': passive
				}

		async_declare_queue_binding_dict = {
				'routing_key': routing_key,
				'arguments': bind_args

				}

		try:
			# Declare exchange and queue
			exchange = await self._async_declare_exchange(**async_declare_exhange_dict)
			queue = await self._async_declare_queue(**async_declare_queue_dict)
			await self._async_declare_queue_binding(queue=queue, exchange=exchange, **async_declare_queue_binding_dict)
			self.auto_declare_ok = True
			log.info(f"Exchange {exchange_name} and Queue {queue_name} set up successfully.")
			if dlx_enable:
				log.info(f"You have a dead letter exhange {dlx_name} for fault tolerance -- use it well young grasshopper!")
			return queue
		except MrsalSetupError as e:
			log.error(f'Splæt! I failed the declaration setup with {e}', exc_info=True)
			self.auto_declare_ok = False


	def _declare_exchange(self,
							 exchange: str, exchange_type: str,
							 arguments: dict[str, str] | None,
							 durable: bool, passive: bool,
							 internal: bool, auto_delete: bool
							) -> None:
		"""This method creates an exchange if it does not already exist, and if the exchange exists, verifies that it is of the correct and expected class.

		If passive set, the server will reply with Declare-Ok if the exchange already exists with the same name,
		and raise an error if not and if the exchange does not already exist, the server MUST raise a channel exception with reply code 404 (not found).

		:param str exchange: The exchange name
		:param str exchange_type: The exchange type to use
		:param bool passive: Perform a declare or just check to see if it exists
		:param bool durable: Survive a reboot of RabbitMQ
		:param bool auto_delete: Remove when no more queues are bound to it
		:param bool internal: Can only be published to by other exchanges
		:param dict arguments: Custom key/value pair arguments for the exchange
		:rtype: `pika.frame.Method` having `method` attribute of type `spec.Exchange.DeclareOk`
		"""
		exchange_declare_info = f"""
								exchange={exchange},
								exchange_type={exchange_type},
								durable={durable},
								passive={passive},
								internal={internal},
								auto_delete={auto_delete},
								arguments={arguments}
								"""
		if self.verbose:
			log.info(f"Declaring exchange with: {exchange_declare_info}")
		try:
			self._channel.exchange_declare(
				exchange=exchange, exchange_type=exchange_type,
				arguments=arguments, durable=durable,
				passive=passive, internal=internal,
				auto_delete=auto_delete
				)
		except Exception as e:
			raise MrsalSetupError(f'Oooopise! I failed declaring the exchange with : {e}')
		if self.verbose:
			log.info("Exchange declared yo!")

	async def _async_declare_exchange(self,
									  exchange: str,
									  exchange_type: AioExchangeType,
									  arguments: dict[str, str] | None = None,
									  durable: bool = True,
									  passive: bool = False,
									  internal: bool = False,
									  auto_delete: bool = False) -> AioExchange:
		"""Declare a RabbitMQ exchange in async mode."""
		exchange_declare_info = f"""
								exchange={exchange},
								exchange_type={exchange_type},
								durable={durable},
								passive={passive},
								internal={internal},
								auto_delete={auto_delete},
								arguments={arguments}
								"""
		if self.verbose:
			print(f"Declaring exchange with: {exchange_declare_info}")

		try:
			exchange_obj = await self._channel.declare_exchange(
				name=exchange,
				type=exchange_type,
				durable=durable,
				auto_delete=auto_delete,
				internal=internal,
				arguments=arguments
			)
			return exchange_obj
		except Exception as e:
			raise MrsalSetupError(f"Failed to declare async exchange: {e}")

	def _declare_queue(self,
					queue: str, arguments: dict[str, str] | None,
					durable: bool, exclusive: bool,
					auto_delete: bool, passive: bool
					) -> None:
		"""Declare queue, create if needed. This method creates or checks a queue.
		When creating a new queue the client can specify various properties that control the durability of the queue and its contents,
		and the level of sharing for the queue.

		Use an empty string as the queue name for the broker to auto-generate one.
		Retrieve this auto-generated queue name from the returned `spec.Queue.DeclareOk` method frame.

		:param str queue: The queue name; if empty string, the broker will create a unique queue name
		:param bool passive: Only check to see if the queue exists and raise `ChannelClosed` if it doesn't
		:param bool durable: Survive reboots of the broker
		:param bool exclusive: Only allow access by the current connection
		:param bool auto_delete: Delete after consumer cancels or disconnects
		:param dict arguments: Custom key/value arguments for the queue
		:returns: Method frame from the Queue.Declare-ok response
		:rtype: `pika.frame.Method` having `method` attribute of type `spec.Queue.DeclareOk`
		"""
		queue_declare_info = f"""
								queue={queue},
								durable={durable},
								exclusive={exclusive},
								auto_delete={auto_delete},
								arguments={arguments}
								"""
		if self.verbose:
			log.info(f"Declaring queue with: {queue_declare_info}")

		try:
			self._channel.queue_declare(queue=queue, arguments=arguments, durable=durable, exclusive=exclusive, auto_delete=auto_delete, passive=passive)
		except Exception as e:
			raise MrsalSetupError(f'Oooopise! I failed declaring the queue with : {e}')
		if self.verbose:
			log.info(f"Queue declared yo")

	async def _async_declare_queue(self,
								   queue_name: str,
								   durable: bool = True,
								   exclusive: bool = False,
								   auto_delete: bool = False,
								   passive: bool = False,
								   arguments: dict[str, Any] | None = None) -> AioQueue:
		"""Declare a RabbitMQ queue asynchronously."""
		queue_declare_info = f"""
								queue={queue_name},
								durable={durable},
								exclusive={exclusive},
								auto_delete={auto_delete},
								arguments={arguments}
								"""
		if self.verbose:
			log.info(f"Declaring queue with: {queue_declare_info}")

		try:
			queue_obj = await self._channel.declare_queue(
				name=queue_name,
				durable=durable,
				exclusive=exclusive,
				auto_delete=auto_delete,
				arguments=arguments,
				passive=passive
			)
			return queue_obj
		except Exception as e:
			raise MrsalSetupError(f"Failed to declare async queue: {e}")

	def _declare_queue_binding(self,
							exchange: str, queue: str,
							routing_key: str | None,
							arguments: dict[str, str] | None
							) -> None:
		"""Bind queue to exchange.

		:param str queue: The queue to bind to the exchange
		:param str exchange: The source exchange to bind to
		:param str routing_key: The routing key to bind on
		:param dict arguments: Custom key/value pair arguments for the binding

		:returns: Method frame from the Queue.Bind-ok response
		:rtype: `pika.frame.Method` having `method` attribute of type `spec.Queue.BindOk`
		"""
		if self.verbose:
			log.info(f"Binding queue to exchange: queue={queue}, exchange={exchange}, routing_key={routing_key}")

		try:
			self._channel.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key, arguments=arguments)
			if self.verbose:
				log.info(f"The queue is bound to exchange successfully: queue={queue}, exchange={exchange}, routing_key={routing_key}")
		except Exception as e:
			raise MrsalSetupError(f'I failed binding the queue with : {e}')
		if self.verbose:
			log.info(f"Queue bound yo")

	async def _async_declare_queue_binding(self,
										   queue: AioQueue,
										   exchange: AioExchange,
										   routing_key: str | None,
										   arguments: dict[str, Any] | None = None) -> None:
		"""Bind the queue to the exchange asynchronously."""
		binding_info = f"""
						queue={queue.name},
						exchange={exchange.name},
						routing_key={routing_key},
						arguments={arguments}
						"""
		if self.verbose:
			log.info(f"Binding queue to exchange with: {binding_info}")

		try:
			await queue.bind(exchange, routing_key=routing_key, arguments=arguments)
		except Exception as e:
			raise MrsalSetupError(f"Failed to bind async queue: {e}")

	def _ssl_setup(self) -> SSLContext:
		"""_ssl_setup is private method we are using to connect with rabbit server via signed certificates and some TLS settings.

		Parameters
		----------

		Returns
		-------
		SSLContext

		"""
		context = ssl.create_default_context(cafile=self.tls_dict['ca'])
		context.load_cert_chain(certfile=self.tls_dict['crt'], keyfile=self.tls_dict['key'])
		return context

	def get_ssl_context(self, async_conn: bool = True) -> SSLOptions | SSLContext | None:
		if self.ssl:
			log.info("Setting up TLS connection")
			context = self._ssl_setup()
			# use_blocking is the same as sync
			if not async_conn:
				ssl_options = pika.SSLOptions(context, self.host)
				return ssl_options
			else:
				return context
		else:
			return None

	def validate_payload(self, payload: Any, model: Type) -> None:
		"""
		Parses and validates the incoming message payload using the provided dataclass model.
		:param payload: The message payload which could be of any type (str, bytes, dict, etc.).
		:param model: The pydantic dataclass model class to validate against.
		:return: An instance of the model if validation is successful, otherwise None.
		"""
		# If payload is bytes, decode it to a string
		if isinstance(payload, bytes):
			payload = payload.decode('utf-8')

		# If payload is a string, attempt to load it as JSON
		if isinstance(payload, str):
			payload = json.loads(payload)  # Converts JSON string to a dictionary

		# Validate the payload against the provided model
		if isinstance(payload, dict):
			model(**payload)
		else:
			raise TypeError("Fool, we aint supporting this type yet {type(payload)}.. Bytes or str -- get it straight")

	def _get_retry_count(self, properties) -> int:
		"""Extract retry count from message headers."""
		if hasattr(properties, 'headers') and properties.headers:
			return properties.headers.get('x-retry-count', 0)
		return 0

	def _has_dlx_configured(self, queue_name: str) -> bool:
		"""Check if the queue has a dead letter exchange configured."""
		return self.dlx_enable

	def _get_retry_cycle_info(self, properties) -> dict:
		"""Extract retry cycle information from message headers."""
		if not hasattr(properties, 'headers') or not properties.headers:
			return {'cycle_count': 0, 'first_failure': None, 'total_elapsed': 0}

		headers = properties.headers
		return {
			'cycle_count': headers.get('x-cycle-count', 0),
			'first_failure': headers.get('x-first-failure'),
			'total_elapsed': headers.get('x-total-elapsed', 0)
		}

	def _should_continue_retry_cycles(self, retry_info: dict, enable_retry_cycles: bool, 
									 max_retry_time_limit: int) -> bool:
		"""Check if message should continue retry cycles or go to permanent DLX."""
		if not enable_retry_cycles or not self.dlx_enable:
			return False

		max_time_ms = max_retry_time_limit * 60 * 1000
		return retry_info['total_elapsed'] < max_time_ms

	def _create_retry_cycle_headers(self, original_headers: dict, cycle_count: int,
								   first_failure: str, processing_error: str,
								   should_cycle: bool, original_exchange: str,
								   original_routing_key: str) -> dict:
		"""Create headers for DLX message with retry cycle info."""
		headers = original_headers.copy() if original_headers else {}
		now = datetime.now(timezone.utc).isoformat()

		# Calculate elapsed time
		if first_failure:
			try:
				first_time = datetime.fromisoformat(first_failure.replace('Z', ''))
				elapsed_ms = int((datetime.now(timezone.utc) - first_time).total_seconds() * 1000)
			except:
				elapsed_ms = 0
		else:
			first_failure = now
			elapsed_ms = 0

		# Update retry cycle tracking
		headers.update({
			'x-cycle-count': cycle_count + 1,
			'x-first-failure': first_failure,
			'x-total-elapsed': elapsed_ms,
			'x-processing-error': processing_error,
			'x-retry-exhausted': not should_cycle
		})

		# If cycling, set TTL and routing back to original queue
		if should_cycle:
			headers.update({
				'x-dead-letter-exchange': original_exchange,
				'x-dead-letter-routing-key': original_routing_key
			})

		return headers

	def _handle_dlx_with_retry_cycle_sync(
			self, method_frame, properties, body, processing_error: str,
			original_exchange: str, original_routing_key: str,
			enable_retry_cycles: bool, retry_cycle_interval: int,
            max_retry_time_limit: int, dlx_exchange_name: str | None):
		"""Base method for DLX handling with retry cycles."""
		# Get retry info
		retry_info = self._get_retry_cycle_info(properties)
		should_cycle = self._should_continue_retry_cycles(retry_info, enable_retry_cycles, max_retry_time_limit)

		# Get DLX info
		dlx_name = dlx_exchange_name or f"{original_exchange}.dlx"
		dlx_routing = original_routing_key

		# Create enhanced headers
		original_headers = getattr(properties, 'headers', {}) or {}
		enhanced_headers = self._create_retry_cycle_headers(
			original_headers, retry_info['cycle_count'], retry_info['first_failure'],
			processing_error, should_cycle, original_exchange, original_routing_key
		)

		# Create properties for DLX message
		dlx_properties = {
			'headers': enhanced_headers,
			'delivery_mode': 2,  # Persistent
			'content_type': getattr(properties, 'content_type', 'application/json')
		}

		# Set TTL if cycling
		if should_cycle:
			ttl_ms = retry_cycle_interval * 60 * 1000
			dlx_properties['expiration'] = str(ttl_ms)

		# Call subclass-specific publish method
		self._publish_to_dlx(dlx_name, dlx_routing, body, dlx_properties)

		# Log result
		if should_cycle:
			log.info(f"Message sent to DLX for retry cycle {retry_info['cycle_count'] + 1} "
					f"(next retry in {retry_cycle_interval}m)")
		else:
			log.error(f"Message permanently failed after {retry_info['cycle_count']} cycles "
					 f"- staying in DLX for manual replay")

	async def _handle_dlx_with_retry_cycle_async(
			self, message, properties, processing_error: str,
			original_exchange: str, original_routing_key: str,
			enable_retry_cycles: bool, retry_cycle_interval: int,
            max_retry_time_limit: int, dlx_exchange_name: str | None):
		"""Base method for DLX handling with retry cycles."""
		# Get retry info
		retry_info = self._get_retry_cycle_info(properties)
		should_cycle = self._should_continue_retry_cycles(retry_info, enable_retry_cycles, max_retry_time_limit)

		# Get DLX info
		dlx_name = dlx_exchange_name or f"{original_exchange}.dlx"
		dlx_routing = original_routing_key

		# Create enhanced headers
		original_headers = getattr(properties, 'headers', {}) or {}
		enhanced_headers = self._create_retry_cycle_headers(
			original_headers, retry_info['cycle_count'], retry_info['first_failure'],
			processing_error, should_cycle, original_exchange, original_routing_key
		)

		# Create properties for DLX message
		dlx_properties = {
			'headers': enhanced_headers,
			'delivery_mode': 2,  # Persistent
			'content_type': getattr(properties, 'content_type', 'application/json')
		}

		# Set TTL if cycling
		if should_cycle:
			ttl_ms = retry_cycle_interval * 60 * 1000
			dlx_properties['expiration'] = str(ttl_ms)

		# Call subclass-specific publish method
		await self._publish_to_dlx(dlx_name, dlx_routing, message.body, dlx_properties)

		# Log result
		if should_cycle:
			log.info(f"Message sent to DLX for retry cycle {retry_info['cycle_count'] + 1} "
					f"(next retry in {retry_cycle_interval}m)")
		else:
			log.error(f"Message permanently failed after {retry_info['cycle_count']} cycles "
					 f"- staying in DLX for manual replay")

	def _publish_to_dlx(self, dlx_exchange: str, routing_key: str, body: bytes, properties: dict):
		"""Abstract method - implemented by subclasses."""
		raise NotImplementedError("Subclasses must implement _publish_to_dlx")
