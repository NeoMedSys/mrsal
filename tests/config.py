from typing import Tuple
TEST_MESSAGE: str = '4f02964a-876a-419c-b309-d784da4b040a'
TEST_MESSAGE_INDEX: int = 3

HOST: str = 'localhost'
PORT: int = 5673
QUEUE: str = 'emergency_queue'
DEAD_LETTER_QUEUE: str = 'dl_queue'

EXCHANGE: str = 'emergency_exchange'
EXCHANGE_TYPE: str = 'direct'
ROUTING_KEY: str = 'emergency_routing_key'

DELAY_EXCHANGE: str = 'delay_exchange'
DELAY_ROUTING_KEY: str = 'delay_routing_key'

DEAD_LETTER_EXCHANGE: str = 'dead_letter_exchange'
DEAD_LETTER_ROUTING_KEY: str = 'dead_letter_routing_key'

MESSAGE_TTL: int = 2000  # ms
