import pika

import mrsal.config.config as config


def is_redelivery_configured(msg_prop: pika.spec.BasicProperties):
    if hasattr(msg_prop, config.MESSAGE_HEADERS_KEY):
        headers = msg_prop.headers
        return headers is not None and config.RETRY_LIMIT_KEY in headers \
            and config.RETRY_KEY in headers
    return False
