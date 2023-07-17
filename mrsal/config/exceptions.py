"""
This is script for custom exceptions
"""


class RabbitMQConnectionError(Exception):
    """Fail to connect to RabbitMQ"""


class RabbitMQDeclareExchangeError(Exception):
    """Fail to declare exchange"""


class RabbitMQDeclareQueueError(Exception):
    """Fail to declare queue"""
