from typing import Tuple, Dict

RABBITMQ_SERVICE_NAME_DOCKER_COMPOSE: str = 'rabbitmq_server'  # Service name in docker-compose.yaml
RABBITMQ_SERVER: str = 'localhost'
V_HOST: str = 'v_host'
RABBITMQ_PORT: int = 5673

RABBITMQ_USER = 'root'
RABBITMQ_PASSWORD = 'password'
RABBITMQ_CREDENTIALS: Tuple[str, str] = (RABBITMQ_USER, RABBITMQ_PASSWORD)

RABBITMQ_EXCHANGE: str = 'emergency_exchange'
RABBITMQ_EXCHANGE_TYPE: str = 'direct'
RABBITMQ_BIND_ROUTING_KEY: str = 'emergency'
RABBITMQ_QUEUE: str = 'emergency_queue'
RABBITMQ_DEAD_LETTER_ROUTING_KEY: str = 'dead_letter'
RABBITMQ_DEAD_LETTER_QUEUE: str = 'dead_letter-queue'

DELAY_EXCHANGE_TYPE: str = 'x-delayed-message'
DELAY_EXCHANGE_ARGS: Dict[str, str] = {'x-delayed-type': 'direct'}
DEAD_LETTER_QUEUE_ARGS: Dict[str, str] = {'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': ''}
