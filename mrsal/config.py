import os
from datetime import datetime
from typing import TYPE_CHECKING, Any
from pydantic import BaseModel

if TYPE_CHECKING:
    from aio_pika import IncomingMessage


class ValidateTLS(BaseModel):
    crt: str
    key: str
    ca: str

try:
	LOG_DAYS: int = int(os.environ.get('LOG_DAYS', 10))
except ValueError:
	LOG_DAYS: int = 10


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
