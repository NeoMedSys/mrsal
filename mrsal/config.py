from datetime import datetime
from typing import TYPE_CHECKING, Any
from pydantic import BaseModel

if TYPE_CHECKING:
	from aio_pika import IncomingMessage


class ValidateTLS(BaseModel):
	crt: str
	key: str
	ca: str


# DLX retry-cycle defaults. Sourced here so the consumer entry points and
# the setup paths stay in sync — they used to duplicate the literal 10 / 60.
DEFAULT_RETRY_CYCLE_INTERVAL_MIN: int = 10
DEFAULT_MAX_RETRY_TIME_LIMIT_MIN: int = 8 * 60  # 8 hours

# Backoff growth for DLX retry cycles. "exponential" computes a per-message
# expiration of base * 2**cycle_count (clamped at DEFAULT_RETRY_BACKOFF_MAX_MIN)
# with ±20% jitter; "fixed" keeps the original queue-TTL-driven flat interval.
# The .retry queue's x-message-ttl is set to the cap so the per-message
# expiration always wins (RabbitMQ honors the shorter of the two).
DEFAULT_RETRY_BACKOFF: str = "exponential"
DEFAULT_RETRY_BACKOFF_MAX_MIN: int = 60  # 1 hour

# Naming convention for the two-queue retry topology. The same suffix is
# applied to both the DLX exchange name and the .dlx queue name (and similarly
# for retry), so operators can identify each component's role at a glance.
DLX_SUFFIX: str = ".dlx"
RETRY_SUFFIX: str = ".retry"

# Exchange types where the .retry binding key is honored. fanout and headers
# exchanges ignore routing keys, so retry/.dlx separation collapses and the
# message gets re-cycled even after the retry budget is exhausted. The
# consumer entry points reject ``enable_retry_cycles=True`` for the others.
RETRY_SAFE_EXCHANGE_TYPES: frozenset[str] = frozenset({"direct", "topic"})


class AioPikaAttributes(BaseModel):
	"""Mirrors the subset of pika.BasicProperties that callbacks may read.

	Populated from aio_pika.IncomingMessage so callbacks receive the same
	field surface across blocking and async consumers.
	"""
	message_id: str | None = None
	app_id: str | None = None
	headers: dict[str, Any] | None = None
	correlation_id: str | None = None
	reply_to: str | None = None
	content_type: str | None = None
	content_encoding: str | None = None
	delivery_mode: int | None = None
	expiration: str | None = None
	priority: int | None = None
	timestamp: datetime | None = None
	type: str | None = None
	user_id: str | None = None

	@classmethod
	def from_message(cls, message: "IncomingMessage") -> "AioPikaAttributes":
		"""Build from an aio_pika.IncomingMessage.

		Uses model_construct to skip re-validation — the source object is
		already typed by aio_pika, and re-validating each field per delivery
		would add per-message overhead with no real benefit.
		"""
		return cls.model_construct(
			message_id=message.message_id,
			app_id=message.app_id,
			headers=message.headers,
			correlation_id=message.correlation_id,
			reply_to=message.reply_to,
			content_type=message.content_type,
			content_encoding=message.content_encoding,
			delivery_mode=message.delivery_mode,
			expiration=message.expiration,
			priority=message.priority,
			timestamp=message.timestamp,
			type=message.type,
			user_id=message.user_id,
		)
