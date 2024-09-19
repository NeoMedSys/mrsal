import os
from pydantic import BaseModel


class ValidateTLS(BaseModel):
    crt: str
    key: str
    ca: str

LOG_DAYS: int = int(os.environ.get('LOG_DAYS', 10))


class AioPikaAttributes(BaseModel):
    message_id: str | None
    app_id: str | None


class MrsalNoAsyncioLoopFound(Exception):
    """Throw this error when user does not inlude asyncio.run"""
