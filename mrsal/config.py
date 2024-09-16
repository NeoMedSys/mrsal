import os
from pydantic import BaseModel


class ValidateTLS(BaseModel):
    crt: str
    key: str
    ca: str

LOG_DAYS: int = int(os.environ.get('LOG_DAYS', 10))
