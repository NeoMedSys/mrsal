# MRSAL  <img align="right" width="125" alt="20200907_104224" src="https://github.com/NeoMedSys/mrsal/assets/9555305/ffb55c86-1e9d-4806-b1ca-c906d6985e3a">
[![Release](https://img.shields.io/badge/release-v0.6.3--alpha-blue.svg)](https://pypi.org/project/mrsal/) [![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3103/) [![Documentation](https://img.shields.io/badge/doc-latest-blue.svg)](https://github.com/NeoMedSys/mrsal/blob/main/FullGuide.md)
<!-- [![Build Status](https://github.com/NeoMedSys/fatty-model-deploy/actions/workflows/tester.yml/badge.svg)](https://github.com/NeoMedSys/fatty-model-deploy/actions/runs/5576890234) -->
[![Tests Status](https://github.com/NeoMedSys/mrsal/assets/9555305/5d29b991-1e7f-4737-8e55-5ee2bc9bb5db)](./reports/junit/junit.xml) [![Coverage Status](https://github.com/NeoMedSys/mrsal/assets/9555305/852b4256-d52e-49da-9134-f0e7381935ba)](./reports/coverage/htmlcov/index.html) [![Flake8 Status](https://github.com/NeoMedSys/mrsal/assets/9555305/0113f279-7c5c-4ed8-bd2f-8b2cf84da2a9)](./reports/flake8/flake8.txt)

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
        headers={'auto_ack': True})

message_body = 'Shalom habibi'

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
    if 'Shalom habibi' in str_message:
        app_id = properties.app_id
        msg_id = properties.message_id
        print(f'app_id={app_id}, msg_id={msg_id}')
        print('Salaam habibi')
        return True  # Consumed message processed correctly
    return False

def consumer_callback(host_param: str, queue_param: str, message_param: str):
    str_message = json.loads(message_param).replace('"', '')
    if 'Shalom habibi' in str_message:
        print('Salaam habibi')
        return True  # Consumed message processed correctly
    return False

mrsal.start_consumer(
        queue='friendship_queue',
        callback=consumer_callback_with_delivery_info,
        callback_args=(test_config.HOST, 'friendship_queue'),
        inactivity_timeout=1,
        requeue=False,
        fast_setup=True,
        callback_with_delivery_info=True
    )
```

Done! Your first message of friendship has been sent to the friendship queue on the exchange of friendship.


### 3 Concurrent Consumers

Sometimes we need to start multiple consumers to listen to the **same** **queue** and process received messages **concurrently**. 
You can do that by calling `start_concurrence_consumer` which takes `total_threads` param in addition to the same parameters used in `start_consumer`.
This method will create a **thread pool** and _spawn new_ `Mrsal` object and start **new consumer** for every thread. 

```python
import json
import time

import pika
from pika.exchange_type import ExchangeType

import mrsal.config.config as config
import tests.config as test_config
from mrsal.mrsal import Mrsal


mrsal = Mrsal(host=test_config.HOST,
              port=config.RABBITMQ_PORT,
              credentials=config.RABBITMQ_CREDENTIALS,
              virtual_host=config.V_HOST)
mrsal.connect_to_server()

APP_ID="TEST_CONCURRENT_CONSUMERS"
EXCHANGE="GoodFriends"
EXCHANGE_TYPE='direct'
QUEUE_EMERGENCY="alleSindInkludiert"  # place the excluded (but no fundamentalist danke) in an emergency queue  
NUM_THREADS=3
NUM_MESSAGES=3
INACTIVITY_TIMEOUT=30 # time out after 30 seconds
ROUTING_KEY="bleib-cool"
MESSAGE_ID="Bleib cool und alles wird besser"

def test_concurrent_consumer():
    # Start concurrent consumers
    mrsal.start_concurrence_consumer(total_threads=NUM_THREADS, queue=QUEUE_EMERGENCY,
                                     callback=consumer_callback_with_delivery_info,
                                     callback_args=(test_config.HOST, QUEUE_EMERGENCY),
                                     exchange=EXCHANGE, exchange_type=EXCHANGE_TYPE,
                                     routing_key=ROUTING_KEY,
                                     inactivity_timeout=INACTIVITY_TIMEOUT,
                                     fast_setup=True,
                                     callback_with_delivery_info=True)

    mrsal.close_connection()

def consumer_callback_with_delivery_info(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    time.sleep(5)
    return True
```

That simple! You have now setup a full advanced message queueing protocol that you can use to promote friendship or other necessary communication between your services.

###### Note! Please refer to the >>>`FULL GUIDE`<<< on how to use customize Mrsal to meet specific needs. There are many parameters and settings that you can use to set up a more sophisticated communication protocol.
---
## References

- [RabbitMQ Tutorials](https://www.rabbitmq.com/getstarted.html)
- [RabbitMQ Exchange Types: 6 Categories Explained Easy](https://hevodata.com/learn/rabbitmq-exchange-type/)
- [What is a Delayed Message Exchange?](https://www.cloudamqp.com/blog/what-is-a-delayed-message-exchange-in-rabbitmq.html#:~:text=The%20RabbitMQ%20delayed%20exchange%20plugin,in%20milliseconds%20can%20be%20specified.)
- [RabbitMQ Delayed Message Plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)
- [When and how to use the RabbitMQ Dead Letter Exchange](https://www.cloudamqp.com/blog/when-and-how-to-use-the-rabbitmq-dead-letter-exchange.html)
- [What is a RabbitMQ vhost?](https://www.cloudamqp.com/blog/what-is-a-rabbitmq-vhost.html)
- [Message Brokers](https://www.ibm.com/cloud/learn/message-brokers)
- [How to Use map() with the ThreadPoolExecutor in Python](https://superfastpython.com/threadpoolexecutor-map/)
- [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html)
- [mrsal_icon](https://www.pngegg.com/en/png-mftic)
---
