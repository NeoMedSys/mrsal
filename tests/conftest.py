from pydantic.dataclasses import dataclass
import warnings

# Suppress RuntimeWarnings for unawaited coroutines globally during tests
warnings.filterwarnings("ignore", message="coroutine '.*' was never awaited", category=RuntimeWarning)


SETUP_ARGS = {
    'host': 'localhost',
    'port': 5672,
    'credentials': ('user', 'password'),
    'virtual_host': 'testboi',
    'prefetch_count': 1
}

@dataclass
class ExpectedPayload:
    id: int
    name: str
    active: bool
