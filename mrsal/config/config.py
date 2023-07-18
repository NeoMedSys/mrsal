import os
from typing import Dict, Tuple

# Service name in docker-compose.yaml
RABBITMQ_SERVICE_NAME_DOCKER_COMPOSE: str = os.environ.get('RABBITMQ_SERVICE_NAME')
RABBITMQ_SERVER: str = 'localhost'
V_HOST: str = os.environ.get('RABBITMQ_DEFAULT_VHOST', 'myMrsalHost')
RABBITMQ_PORT: int = os.environ.get('RABBITMQ_PORT', 5672)
RABBITMQ_PORT_TLS: int = os.environ.get('RABBITMQ_PORT_TLS', 5671)
RABBIT_DOMAIN: str = os.environ.get('RABBITMQ_DOMAIN', 'localhost')

RABBITMQ_USER = os.environ.get('RABBITMQ_DEFAULT_USER', 'root')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_DEFAULT_PASS', 'password')
RABBITMQ_CREDENTIALS: Tuple[str, str] = (RABBITMQ_USER, RABBITMQ_PASSWORD)

RABBITMQ_EXCHANGE: str = 'emergency_exchange'
RABBITMQ_EXCHANGE_TYPE: str = 'direct'
RABBITMQ_BIND_ROUTING_KEY: str = 'emergency'
RABBITMQ_QUEUE: str = 'emergency_queue'
RABBITMQ_DEAD_LETTER_ROUTING_KEY: str = 'dead_letter'
RABBITMQ_DEAD_LETTER_QUEUE: str = 'dead_letter-queue'

DELAY_EXCHANGE_TYPE: str = 'x-delayed-message'
DELAY_EXCHANGE_ARGS: Dict[str, str] = {'x-delayed-type': 'direct'}
DEAD_LETTER_QUEUE_ARGS: Dict[str, str] = {
    'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': ''}

CONTENT_TYPE: str = 'text/plain'
CONTENT_ENCODING: str = 'utf-8'

RETRY_LIMIT_KEY: str = 'x-retry-limit'
RETRY_KEY: str = 'x-retry'
MESSAGE_HEADERS_KEY: str = 'headers'
