from typing import Tuple

RABBITMQ_SERVICE_NAME_DOCKER_COMPOSE: str = 'rabbitmq_server'  # Service name in docker-compose.yaml
RABBITMQ_SERVER: str = 'localhost'
RABBITMQ_DEFAULT_PORT: int = 5673

RABBITMQ_USER = 'root'  # DELETEME
RABBITMQ_PASSWORD = 'password'  # DELETEME
RABBITMQ_CREDENTIALS: Tuple[str, str] = (RABBITMQ_USER, RABBITMQ_PASSWORD)

RABBITMQ_EXCHANGE: str = 'bloody-exchange'
RABBITMQ_EXCHANGE_TYPE: str = 'direct'  # pick from direct, headers, topic or fanout
RABBITMQ_BIND_ROUTING_KEY: str = 'emergency'
RABBITMQ_QUEUE: str = 'bloody-queue' 
RABBITMQ_DEAD_LETTER_ROUTING_KEY: str = 'dead-letter'
RABBITMQ_DEAD_LETTER_QUEUE: str = 'dead-letter-queue'  

NON_PERSIST_MSG: int = 1
PERSIST_MSG: int = 2

HOST = 'localhost'
PORT = 4003

PROJECT_ID = 'bloody'
REALTIME_API_PATH = '/process/realtime/'

OK_STATUS_CODE = 200
