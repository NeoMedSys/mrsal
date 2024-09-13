from pydantic.dataclasses import dataclass

SETUP_ARGS = {
    'host': 'localhost',
    'port': 5672,
    'credentials': ('user', 'password'),
    'virtual_host': 'testboi'
}

@dataclass
class ExpectedPayload:
    id: int
    name: str
    active: bool
