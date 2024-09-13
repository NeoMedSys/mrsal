from pydantic import BaseModel


class ValidateTLS(BaseModel):
    'tls.crt': str
    'tls.key': str
    'tls.ca': str
