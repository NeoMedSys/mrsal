from pydantic.dataclasses import dataclass

SETUP_ARGS = {
    'host': 'localhost',
    'port': 5672,
    'credentials': ('user', 'password'),
    'virtual_host': 'testboi',
    'prefetch_count': 1,
    'heartbeat': 60
}

@dataclass
class ExpectedPayload:
    id: int
    name: str
    active: bool
