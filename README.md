# MRSAL  <img align="right" width="125" alt="20200907_104224" src="https://user-images.githubusercontent.com/29639563/187228621-af1d695d-29a3-4940-9a8c-c19bcd6421a5.png">
<img src="https://img.shields.io/badge/release-v0.1.0--alpha-blue" height="20" /> [![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/)
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/) 

## Intro
Mrsal is a simple to use message broker abstraction on top of [RabbitMQ](https://www.rabbitmq.com/) and [Pika](https://pika.readthedocs.io/en/stable/index.html). The goal is to make Mrsal trivial to re-use in all services of a distributed system and to make the use of advanced message queing protocols easy and safe. No more big chunks of repetive code across your services or bespoke solutions to handle dead letters. 

###### Mrsal is Arabic for a small arrow and is used to describe something that performs a task with lightness and speed. 

## Quick Start guide

### 0. Install

First things first: 

```bash
pip install mrsal
```

We need to install RabbitMQ to use Mrsal. Head over to [install](https://www.rabbitmq.com/download.html) RabbitMQ. Make sure to stick to the configuration that you give the installation throughout this guide. You can also use the [Dockerfile](https://github.com/NeoMedSys/mrsal/blob/main/Dockerfile) and the [docker-compose](https://github.com/NeoMedSys/mrsal/blob/main/docker-compose.yml) that we are using in the full guide.

Next set the default username, password and servername for your RabbitMQ setup. It's advisable to use a `.env` script or the rc file for persistence.

```bash
[RabbitEnvVars]
RABBITMQ_DEFAULT_USER=******
RABBITMQ_DEFAULT_PASS=******
RABBITMQ_DEFAULT_VHOST=******
RABBITMQ_DOMAIN=******
RABBITMQ_DOMAIN_TLS=******

RABBITMQ_GUI_PORT=******
RABBITMQ_PORT=******
RABBITMQ_PORT_TLS=******

# FOR TLS
RABBITMQ_CAFILE=/path/to/file
RABBITMQ_CERT=/path/to/file
RABBITMQ_KEY=/path/to/file
```

Please read the [full guide](https://github.com/NeoMedSys/mrsal/blob/main/FullGuide.md) to understand what Mrsal currently can and can't do.

###### Mrsal was first developed by NeoMedSys and the research group [CRAI](https://crai.no/) at the univeristy hospital of Oslo.

### 1. Setup and connect


The first thing we need to do is to setup our rabbit server before we can subscribe and publish to it. Lets set up a server on our localhost with the port and credentials we used when spinning up the docker-compose

```python
import json
import pika
from mrsal.mrsal import Mrsal

# If you want to use SSL for external listening then set it to True
SSL = False

# Note RabbitMQ container is listening on:
# 1. When SSL is False the default port 5672 which is exposed to RABBITMQ_PORT in docker-compose
# 2. When SSL is True the default port 5671 which is exposed to RABBITMQ_PORT_TLS in docker-compose
port = RABBITMQ_PORT_TLS if SSL else RABBITMQ_PORT
host = RABBITMQ_DOMAIN_TLS if SSL else RABBITMQ_DOMAIN

# It should match with the env specifications (RABBITMQ_DEFAULT_USER, RABBITMQ_DEFAULT_PASS)
credentials=(RABBITMQ_DEFAULT_USER, RABBITMQ_DEFAULT_PASS)

# It should match with the env specifications (RABBITMQ_DEFAULT_VHOST)
v_host = RABBITMQ_DEFAULT_VHOST

mrsal = Mrsal(
    host=host,
    port=port,
    credentials=credentials,
    virtual_host=v_host,
    ssl=SSL
)

mrsal.connect_to_server()
```

### 2 Publish
Now lets publish our message of friendship on the friendship exchange.
Note: When `fast_setup=True` that means Mrsal will create the specified `exchange` and `queue`, then bind them together using `routing_key`.

```python
# BasicProperties is used to set the message properties
prop = pika.BasicProperties(
        app_id='friendship_app',
        message_id='friendship_msg',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)

message_body = 'Hello'

# Publish the message to the exchange to be routed to queue
mrsal.publish_message(exchange='friendship',
                        exchange_type='direct',
                        queue='friendship_queue',
                        routing_key='friendship_key',
                        message=json.dumps(message_body), 
                        prop=prop,
                        fast_setup=True)
```

### 3 Consume

Now lets setup a consumer that will listen to our very important messages. If you are using scripts rather than notebooks then it's advisable to run consume and publish separately. We are going to need a callback function which is triggered upon receiving the message from the queue we subscribe to. You can use the callback function to activate something in your system.

Note: 
- If you start a consumer with `callback_with_delivery_info=True` then your callback function should have at least these params `(method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str)`. 
- If not, then it should have at least `(message_param: str)`

```python
import json

def consumer_callback_with_delivery_info(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    str_message = json.loads(message_param).replace('"', '')
    if 'Hello' in str_message:
        app_id = properties.app_id
        msg_id = properties.message_id
        print(f'app_id={app_id}, msg_id={msg_id}')
        print('Salaam habibi')
        return True  # Consumed message processed correctly
    return False

def consumer_callback(host_param: str, queue_param: str, message_param: str):
    str_message = json.loads(message_param).replace('"', '')
    if 'Hello' in str_message:
        print('Salaam habibi')
        return True  # Consumed message processed correctly
    return False

mrsal.start_consumer(
        queue='friendship_queue',
        callback=consumer_callback_with_delivery_info,
        callback_args=(test_config.HOST, 'friendship_queue'),
        inactivity_timeout=1,
        requeue=False,
        callback_with_delivery_info=True
    )
```

Done! Your first message of friendship has been sent to the friendship queue on the exchange of friendship.

That simple! You have now setup a full advanced message queueing protocol that you can use to promote friendship or other necessary communication between your services.

###### Note! Please refer to the full guide on how to use customize Mrsal to meet specific needs. There are many parameters and settings that you can use to set up a more sophisticated communication protocol.
