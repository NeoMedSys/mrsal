from pydantic import BaseModel


class ValidateTLS(BaseModel):
    crt: str
    key: str
    ca: str
