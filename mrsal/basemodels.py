from pydantic import BaseModel

class MrsalProtocol(BaseModel):
    exchange_name: str
    queue_name: str
    routing_key: str
    message: str | bytes
    exchange_type: str

