import os
from pydantic import BaseModel


class ValidateTLS(BaseModel):
    crt: str
    key: str
    ca: str

try:
	LOG_DAYS: int = int(os.environ.get('LOG_DAYS', 10))
except ValueError:
	LOG_DAYS: int = 10


class AioPikaAttributes(BaseModel):
    message_id: str | None
    app_id: str | None
    headers: dict | None = None
