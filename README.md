# MRSAL  <img align="right" width="125" alt="20200907_104224" src="https://user-images.githubusercontent.com/29639563/187228621-af1d695d-29a3-4940-9a8c-c19bcd6421a5.png">

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
RABBITMQ_DEFAULT_USER=****
RABBITMQ_DEFAULT_PASSWORD=****
RABBITMQ_DEFAULT_SERVICE_NAME=****
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

### 2. Declare an exchange and queue

#### 2.1 Exchange
There are currently no exchanges declared on `myMrsalHost` so lets declare an exchange we can declare a queue on which we finally can bind and publish our messages to. We are going to use a **direct** exchange in this guide. See the full guide for more detailed information about the types of exhanges you can declare.

```python

mrsal.setup_exchange(
        exchange='friendship',
        exchange_type='direct'
)                 
```

##### 2.2 Queue

Finally we need to declare the queue on the exchange that we want to bind and publish to. This is the last step of setting up a message protocol.

```python
mrsal.setup_queue(queue='friendship_queue')
```

### 3. Bind and publish

#### 3.1 Bind
We have a queue on an exchange that we can start publishing messages to, although we need to bind ourselves to it before we can publish messages at will. So lets do that with one of Mrsal's easy to use methods.

```python
mrsal.setup_queue_binding(
        exchange='friendship',
        queue='friendship_queue',
        routing_key='friendship_key'  # use this string match key to make sure that the messages are delivered to the right exchange.
)
```

#### 3.2 Publish

Finally we are ready to publish our first message to the queue. We are going to specifiy a JSON containing the message properties for RabbitMQ to understand how to parse our message. 


```python
import json

prop = pika.BasicProperties(
    content_type='text/plain',
    content_encoding='utf-8',
    delivery_mode=pika.DeliveryMode.Persistent
)


mrsal.publish_message(
    exchange='friendship',
    routing_key='friendship_key',
    message=json.dumps('Salaam habibi'),
    properties=prop
)
```

Done! Your first message of friendship has been sent to the friendship queue on the exchange of friendship.

### Consume

It would be a pitty if nobody is listening to the message of friendship, so let's make sure that someone is ready to listen to our message. We are going to make a simple callback function, which is the function that is triggered by our message, and start the listening by setting up so called consumer.

```python

def consumer_callback(message: Dict[str, Any]):
        msg = json.load(message)
        if 'Salaam' in msg:
            return 'Shalom habibi'

mrsal.start_consumer(
    queue='friendship_queue,
    callback=consumer_callback,
    callback_args=message
)
```

That's it. You have now setup a full advanced message queueing protocol that you can use to promote friendship or other necessary communication between your services.
