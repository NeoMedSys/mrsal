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
RABBITMQ_DEFAULT_USER=****
RABBITMQ_DEFAULT_PASS=****
RABBITMQ_DEFAULT_SERVICE_NAME=****
RABBITMQ_DEFAULT_VHOST=****
RABBITMQ_CONTAINER_PORT=****
RABBITMQ_GUI_PORT=****
```

Please read the [full guide](https://github.com/NeoMedSys/mrsal/blob/main/FullGuide.md) to understand what Mrsal currently can and can't do.

###### Mrsal was first developed by NeoMedSys and the research group [CRAI](https://crai.no/) at the univeristy hospital of Oslo.

### 1. Setup and connect


The first thing we need to do is to setup our rabbit server before we can subscribe and publish to it. Lets set up a server on our localhost with the port and credentials we used when spinning up the docker-compose

```python
from mrsal import Mrsal

mrsal = Mrsal(
    host='localhost',
    port=5671,
    credentials=('username', 'password'),
    virtual_host='myMrsalHost'  # use this to connect to specific part of the rabbit server
)

mrsal.connect_to_server()
```

### 2 Consume

Before publishing our first message, lets setup a consumer that will listen to our very important messages. If you are using scripts rather than noterbooks than it's advisable to run consume and publish in separately. We are going to need callback functions which is triggered on receiving the message from the exchange and queue we subscribe to.


```python

def consumer_callback(host: str, queque:str, message: str):
        if 'Salaam' in message:
            return 'Shalom habibi'

mrsal.start_consumer(
    exchange='friendship',
    exchange_type='direct',
    routing_key='friendship_key',
    queue='friendship_queue,
    callback=consumer_callback,
    callback_args=('localhost', 'friendship_queque')
)
```

### 3 Publish
Now lets publish our message of friendship on the friendship exchange that a friend is currently listening to.

```python
import json

mrsal.publish_message(
    exchange='friendship',
    exchange_type='direct',
    routing_key='friendship_key',
    queue='friendship_queue',
    message=json.dumps('Salaam habibi')
)
```

Done! Your first message of friendship has been sent to the friendship queue on the exchange of friendship.

That simple! You have now setup a full advanced message queueing protocol that you can use to promote friendship or other necessary communication between your services.

###### Note! Please refer to the full guide on how to use customise Mrsal to meet specific needs. There are many parameters and settings that you can use to set up a more sophisticated communication protocol.
