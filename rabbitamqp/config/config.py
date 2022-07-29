from typing import Tuple, Dict

RABBITMQ_SERVICE_NAME_DOCKER_COMPOSE: str = 'rabbitmq_server'  # Service name in docker-compose.yaml
RABBITMQ_SERVER: str = 'localhost'
V_HOST: str = 'v_host'
RABBITMQ_DEFAULT_PORT: int = 5673

RABBITMQ_USER = 'root'  # DELETEME
RABBITMQ_PASSWORD = 'password'  # DELETEME
RABBITMQ_CREDENTIALS: Tuple[str, str] = (RABBITMQ_USER, RABBITMQ_PASSWORD)

RABBITMQ_EXCHANGE: str = 'bloody_exchange'
RABBITMQ_EXCHANGE_TYPE: str = 'direct'  # pick from direct, headers, topic or fanout
RABBITMQ_BIND_ROUTING_KEY: str = 'emergency'
RABBITMQ_QUEUE: str = 'bloody_queue'
RABBITMQ_DEAD_LETTER_ROUTING_KEY: str = 'dead_letter'
RABBITMQ_DEAD_LETTER_QUEUE: str = 'dead_letter-queue'

NON_PERSIST_MSG: int = 1
PERSIST_MSG: int = 2

HOST = 'localhost'
PORT = 4003

PROJECT_ID = 'bloody'
REALTIME_API_PATH = '/process/realtime/'

OK_STATUS_CODE = 200

DELAY_EXCHANGE_TYPE: str = 'x-delayed-message'  # Unchangeable and needed for plugin "rabbitmq_delayed_message_exchange"
DELAY_EXCHANGE_ARGS: Dict[str, str] = {'x-delayed-type': 'direct'}
DEAD_LETTER_QUEUE_ARGS: Dict[str, str] = {'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': ''}
