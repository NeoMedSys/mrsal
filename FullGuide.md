# MRSAL FULL GUIDE  <img align="right" width="125" alt="20200907_104224" src="https://github.com/NeoMedSys/mrsal/assets/9555305/ffb55c86-1e9d-4806-b1ca-c906d6985e3a">
[![Release](https://img.shields.io/badge/release-v0.7.2-blue.svg)](https://pypi.org/project/mrsal/) [![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-3103/) [![Documentation](https://img.shields.io/badge/doc-latest-blue.svg)](https://github.com/NeoMedSys/mrsal/blob/main/FullGuide.md)
<!-- [![Build Status](https://github.com/NeoMedSys/fatty-model-deploy/actions/workflows/tester.yml/badge.svg)](https://github.com/NeoMedSys/fatty-model-deploy/actions/runs/5576890234) -->
[![Tests Status](https://github.com/NeoMedSys/mrsal/blob/MRSAL-22/reports/badges/tests-badge.svg)](./reports/junit/junit.xml) [![Coverage Status](https://github.com/NeoMedSys/mrsal/blob/MRSAL-22/reports/badges/coverage-badge.svg)](./reports/coverage/htmlcov/index.html) [![Flake8 Status](https://github.com/NeoMedSys/mrsal/blob/MRSAL-22/reports/badges/ruff-badge.svg)](./reports/flake8/flake8.txt)


## Table of Content
- [Introduction](#introduction)
    * [The What](#theWhat)
    * [The Why](#theWhy)
    * [The How](#theHow)
- [Installation](#installation)
- [Start RabbitMQ](#startRabbitMQ)
- [RabbitMQ Message Concepts](#concepts)
- [RabbitMQ Message Cycle](#cycle)
- [Connect To RabbitMQ Server](#connectToServer)
- [Declare Exchange](#declareExchange)
- [Declare Queue](#declareQueue)
- [Bind Queue To Exchange](#bindQueueToExchange)
- [Publish Message](#publishMessage)
- [Start Consumer](#startConsumer)
- [Exchange Types](#exchangeTypes)
    * [Direct Exchange](#directExchange)
    * [Topic Exchange](#topicExchange)
    * [Fanout Exchange](#fanoutExchange)
    * [Headers Exchange](#headersExchange)
    * [Delay Exchange](#delayExchange)
- [Setup Queue With Dead Letters Exchange](#queueWithDeadLetters)
- [Dead and Delay Letters Workflow](#deadAndDelayLetters)
- [Redeliver Rejected Letters With Delay Workflow](#redeliverLettersWithDelay)
- [Quorum Queue With Delivery Limit Workflow](#quorumWithDeliveryLimit)
- [Setup Concurrent Consumers](#concurrentConsumers)
- [Resources](#resources)
---  
<div id='introduction'/>

## Introduction

<div id='theWhat'/>

### The What
> MRSAL Is a _message broker_ based on [**RabbitMQ**](https://www.rabbitmq.com/) with [**Pika**](https://pika.readthedocs.io/en/stable/#).

<div id='theWhy'/>

### The Why
> A message broker is software that enables applications, systems, and services to communicate with each other and exchange information. This allows interdependent services to "talk" with one another directly, even if they were written in different languages or implemented on different platforms.

<div id='theHow'/>

### The How
> The message broker does this by translating messages between these different services.

---
<div id='installation'/>

## Installation

MRSAL is available for download via PyPI and may be installed using pip.
```bash
pip install mrsal
```
---
<div id='startRabbitMQ'/>

## Start RabbitMQ Container

We are using **docker** to start a `RabbitMQ container` listening on the port `"5672"` for localhost and `5671` for SSL with `"Delayed Message Plugin"` installed and enabled. If you want to use SSL for external listnening then you have to create certifactes with e.g. OpenSSL and either have them signed by yourself or an offical authenticator. Lastly you need to add a `rabbitmq.conf` that declares SSL connection with your specifications, see the official [walkthrough](https://www.rabbitmq.com/ssl.html) for guidance. Get the plugin for `x-delayed-message` by dowloading it with `wget` (not curl) and binding it to the docker image. You can find the plugin binary [here](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases)

- env file
```env
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


- docker-compose.yml
```Dockerfile
version: '3.9'

services:
  rabbitmq:
    image: rabbitmq:3.11.6-management-alpine
    container_name: mrsal
    volumes:
      # bind the volume
      - 'rabbitmq_vol:/var/lib/rabbitmq/'
      - 'rabbitmq_vol:/var/log/rabbitmq/'
      # For TLS connection
      - '~/rabbitmq/rabbit-server.crt:/etc/rabbitmq/rabbit-server.crt'
      - '~/rabbitmq/rabbit-server.key:/etc/rabbitmq/rabbit-server.key'
      - '~/rabbitmq/rabbit-ca.crt:/etc/rabbitmq/rabbit-ca.crt'
      # You need to specify the TLS connection for rabbitmq with the config file
      - '~/rabbitmq/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf'
      # This is to enable x-delayed-messages 
      - '~/rabbitmq/rabbitmq_delayed_message_exchange-3.11.1.ez:/opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-3.11.1.ez'
    environment:
      - RABBITMQ_DEFAULT_USER=${RABBITMQ_DEFAULT_USER}
      - RABBITMQ_DEFAULT_PASS=${RABBITMQ_DEFAULT_PASS}
      - RABBITMQ_DEFAULT_VHOST=${RABBITMQ_DEFAULT_VHOST}
    ports:
      # RabbitMQ container listening on the default port of 5672.
      - "${RABBITMQ_PORT}:5672"
      - "${RABBITMQ_PORT_TLS}:5671"
      # OPTIONAL: Expose the GUI port
      - "${RABBITMQ_GUI_PORT}:15672"
    networks:
      - gateway
    restart: always

volumes:
  rabbitmq_vol:
```

- Install image and start RabbitMQ container
```bash
docker compose -f docker-compose.yml up -d
```

- Lastly enable the plugin the docker image
```bash
docker exec -it <docker-image-tag> sh
```
inside the docker image run the enable command
```bash
rabbitmq-plugins enable rabbitmq_delayed_message_exchange
```

---
<div id='concepts'/>

## RabbitMQ Message Concepts

- **Producer** Is a user application that sends messages. Messages are not published directly to a queue; instead, the producer sends messages to an exchange. 
- **Exchange** Is responsible for routing the messages to different queues using header attributes, bindings, and routing keys. 
- **Binding** A binding is a connection that you build between a queue and an exchange.
- **Routing Key** Is a message attribute taken into account by the exchange when deciding how to route a message.
- **Queue** Is a buffer that receives and stores messages until the consumer receives them.
- **Consumer** Is a user application that receives and handles messages.
---
<div id='cycle'/>

## RabbitMQ Message Cycle

<p align="center">
<img src="./doc_images/standard_wf.png" alt="drawing" width="600"/>
</p>

1. The **producer** publishes a message to an exchange.

2. The **exchange** routes the message into the queues bound to it depending on exchange type and routing key.

3. The messages stay in the **queue** until they are handled by a consumer.

4. The **consumer** handles the message.
---
<div id='connectToServer'/>

## Connect To RabbitMQ Server

This tutorial assumes RabbitMQ is installed and running on localhost on the port (5673). In case you use a different host, vhost, port or credentials, connections settings would require adjusting.

- vhost: 
    - Think of vhosts as individual, uniquely named containers.
    - Inside each vhost container is a logical group of exchanges, connections, queues, bindings, user permissions, and other system resources. 
    - Different users can have different permissions to different vhost and queues and exchanges can be created, so they only exist in one vhost. 
    - When a client establishes a connection to the RabbitMQ server, it specifies the vhost within which it will operate
```py
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
---
<div id='declareExchange'/>

## Declare Exchange:

**Exchange** Is responsible for routing the messages to different queues using header attributes, bindings, and routing keys. 
- `exchange`: The exchange name 
- `exchange_type`: The exchange type to use
    - `direct`
    - `topic`
    - `fanout`
    - `headers`
    - `x-delayed-message`
- `passive`: Perform a declare or just check to see if it exists
- `durable`: Survive a reboot of RabbitMQ
- `auto_delete`: Remove when no more queues are bound to it
- `internal`: Can only be published to by other exchanges
- `arguments`: Custom key/value pair arguments for the exchange. E.g:
    - When the type of exchange is `x-delayed-message`, we specify how the messages will be routed after the delay period ([see example](#delayExchange)).
        ```py
        {'x-delayed-type': 'direct'}
        ```
```py
# Argument with the kye x-delayed-type to specify how the messages will be routed after the delay period specified
EXCHANGE_ARGS: str = {'x-delayed-type': 'direct'}

mrsal.setup_exchange(exchange='agreements',
                    exchange_type='x-delayed-message',
                    arguments=EXCHANGE_ARGS,
                    durable=True, passive=False, internal=False, auto_delete=False)
```
---
<div id='declareQueue'/>

## Declare Queue:

**Queue** Is a buffer that receives and stores messages until the consumer receives them.
- `queue`: The queue name; if empty string, the broker will
    create a unique queue name
- `passive`: Only check to see if the queue exists and raise
    _ChannelClosed_ if it doesn't
- `durable`: Survive reboots of the broker
- `exclusive`: Only allow access by the current connection
- `auto_delete`: Delete after consumer cancels or disconnects
- `arguments`: Custom key/value arguments for the queue. E.g:
    - Specify dl exchange and dl routing key for queue
    - Specify an amount of time in ms expressing the time to live for the message in queue before it considered as **dead**. 
    - ([see example](#queueWithDeadLetters))
        ```py
        {'x-dead-letter-exchange': DL_EXCHANGE, 
        'x-dead-letter-routing-key': DL_ROUTING_KEY,
        'x-message-ttl': 2000}
        ```

```py
# Specify dl exchange and dl routing key for queue
QUEUE_ARGS = {'x-dead-letter-exchange': DL_EXCHANGE,
                'x-dead-letter-routing-key': DL_ROUTING_KEY,
                'x-message-ttl': 2000}
mrsal.setup_queue(queue='agreements_queue',
                arguments=QUEUE_ARGS,
                durable=True,
                exclusive=False, auto_delete=False, passive=False)
```
---
<div id='bindQueueToExchange'/>

## Bind Queue To Exchange:

Bind the queue to exchange.

- `queue`: The queue to bind to the exchange
- `exchange`: The source exchange to bind to
- `routing_key`: The routing key to bind on
- `arguments`: Custom key/value pair arguments for the binding. E.g:
    - When exchange's type is `headers`, we need to bound queue to exchange specifying the headers which has to match the published-messages' headers ([see example](#headersExchange)). 

```py
ARGS = {'x-match': 'all', 'format': 'zip', 'type': 'report'}
mrsal.setup_queue_binding(exchange='agreements',
                        routing_key='agreements_key',
                        queue='agreements_queue',
                        arguments=ARGS)
```
---
<div id='publishMessage'/>

## Publish Message

Publish message to the exchange specifying routing key and properties

- `exchange`: The exchange to publish to
- `routing_key`: The routing key to bind on
- `body`: The message body; empty string if no body
- `prop`: BasicProperties is used to set the message properties
- `headers`: Is useful when we want to send message with headers. E.g:
        - When exchange's type is `x-delayed-message` then we need to send messages to the exchange with `x-delay` header to specify delay time for message in exchange before route it to bound queue ([see example](#delayExchange)).
        - When exchange's type is `headers`, then we need to send messages with headers which match the binding-key of bound queues to the exchange ([see example](#headersExchange)).
```py
message: str = 'agreement123'

# publish messages with  header x-delay expressing in milliseconds a delay time for the message.
headers={'x-delay': 2000}, 

# BasicProperties is used to set the message properties
prop = pika.BasicProperties(
        app_id='agreements_app',
        message_id='agreements_msg',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=headers)

mrsal.publish_message(
                exchange='agreements',
                routing_key='agreements_key',
                message=json.dumps(message),
                prop=prop,
                fast_setup=False
                )
```                        
--- 
<div id='startConsumer'/>

## Start Consumer

- Setup consumer:
    - Consumer start consuming the messages from the queue.
    - If `inactivity_timeout` is given the consumer will be canceled when inactivity_timeout is exceeded.
    - If you start a consumer with `callback_with_delivery_info=True` then your callback function should to have at least these params `(method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str)`. If not, then it should have at least `(message_param: str)`
    - Send the consumed message to callback method to be processed, and then the message can be either:
        - Processed, then **correctly-acknowledge** and deleted from queue or 
        - Failed to process, **negatively-acknowledged** and then will be either
            - `Requeued` if requeue is True
            - `Dead letter` and deleted from queue if 
                - requeue is False
                - requeue is True and requeue attempt fails.

```py
def consumer_callback_with_delivery_info(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    str_message = json.loads(message_param).replace('"', '')
    if 'agreement123' in str_message:
        app_id = properties.app_id
        msg_id = properties.message_id
        print(f'app_id={app_id}, msg_id={msg_id}')
        print('Message processed')
        return True  # Consumed message processed correctly
    return False

def consumer_callback(host: str, queue: str, message: str):
    str_message = json.loads(message_param).replace('"', '')
    if 'agreement123' in str_message:
        print('Message processed')
        return True  # Consumed message processed correctly
    return False

QUEUE: str = 'agreements_queue'

mrsal.start_consumer(
        queue='agreements_queue',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'agreements_queue'),
        inactivity_timeout=6,
        requeue=False
    )

# NOTE: If you want to use callback with delivery info then use this code

# mrsal.start_consumer(
#         queue='agreements_queue',
#         callback=consumer_callback_with_delivery_info,
#         callback_args=(test_config.HOST, 'agreements_queue'),
#         inactivity_timeout=6,
#         requeue=False,
#         callback_with_delivery_info=True
#     )
```
---
<div id='exchangeTypes'/>

## Exchange Types

<div id='directExchange'/>

1. **Direct Exchange**

    - Uses a message `routing key` to transport messages to queues. 
    - The `routing key` is a message attribute that the _producer_ adds to the message header. 
    - You can consider the routing key to be an _address_ that the exchange uses to determine how the message should be routed. 
    - A message is delivered to the queue with the `binding key` that **exactly** matches the message’s `routing key`. 

<p align="center">
<img src="./doc_images/exchange_direct_wf.png" alt="drawing" width="600"/>
</p>

```py
def consumer_callback(host_param: str, queue_param: str, message_param: str):
    return True

EXCHANGE: str = 'agreements'
EXCHANGE_TYPE: str = 'direct'
QUEUE_1: str = 'agreements_berlin_queue'
QUEUE_2: str = 'agreements_madrid_queue'

# Messages will published with this routing key
ROUTING_KEY_1: str = 'berlin agreements'
ROUTING_KEY_2: str = 'madrid agreements'
# ------------------------------------------

# Setup exchange
mrsal.setup_exchange(exchange=EXCHANGE,
                    exchange_type=EXCHANGE_TYPE)
# ------------------------------------------

# Setup queue for berlin agreements
mrsal.setup_queue(queue=QUEUE_1)

# Bind queue to exchange with binding key
mrsal.setup_queue_binding(exchange=EXCHANGE,
                        routing_key=ROUTING_KEY_1,
                        queue=QUEUE_1)
# ------------------------------------------

# Setup queue for madrid agreements
mrsal.setup_queue(queue=QUEUE_2)

# Bind queue to exchange with binding key
mrsal.setup_queue_binding(exchange=EXCHANGE,
                        routing_key=ROUTING_KEY_2,
                        queue=QUEUE_2)
# ------------------------------------------

# Publisher:

# Message ("uuid2") is published to the exchange and it's routed to queue2
prop1 = pika.BasicProperties(
        app_id='test_exchange_direct',
        message_id='madrid_uuid',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
message2 = 'uuid2'
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key=ROUTING_KEY_2,
                        message=json.dumps(message2),
                        prop=prop1)

prop2 = pika.BasicProperties(
        app_id='test_exchange_direct',
        message_id='berlin_uuid',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
# Message ("uuid1") is published to the exchange and it's routed to queue1
message1 = 'uuid1'
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key=ROUTING_KEY_1,
                        message=json.dumps(message1),
                        prop=prop2)
# ------------------------------------------

# Start consumer for every queue
mrsal.start_consumer(
    queue=QUEUE_1,
    callback=consumer_callback,
    callback_args=('localhost', QUEUE_1),
    inactivity_timeout=1,
    requeue=False
)

mrsal.start_consumer(
    queue=QUEUE_2,
    callback=consumer_callback,
    callback_args=('localhost', QUEUE_2),
    inactivity_timeout=1,
    requeue=False
)
# ------------------------------------------
```
<div id='topicExchange'/>

2. **Topic Exchange**

    - Topic RabbitMQ exchange type sends messages to queues depending on `wildcard matches` between the `routing key` and the queue binding's `routing pattern`. 
    - `'*'` (star) can substitute for exactly one word.
    - `'#'` (hash) can substitute for zero or more words.
    - The routing patterns may include an asterisk `'*'` to match a word in a specified position of the routing key (for example, a routing pattern of `'agreements.*.*.berlin.*'` only matches routing keys with `'agreements'` as the first word and `'berlin'` as the fourth word).

<p align="center">
<img src="./doc_images/exchange_topic_wf.png" alt="drawing" width="600"/>
</p>

```py
def consumer_callback(host_param: str, queue_param: str, message_param: str):
    return True

EXCHANGE: str = 'agreements'
EXCHANGE_TYPE: str = 'topic'

QUEUE_1: str = 'berlin_agreements'
QUEUE_2: str = 'september_agreements'

ROUTING_KEY_1: str = 'agreements.eu.berlin.august.2022'  # Messages will published with this routing key
ROUTING_KEY_2: str = 'agreements.eu.madrid.september.2022'  # Messages will published with this routing key

BINDING_KEY_1: str = 'agreements.eu.berlin.#'  # Berlin agreements
BINDING_KEY_2: str = 'agreements.*.*.september.#'  # Agreements of september
BINDING_KEY_3: str = 'agreements.#'  # All agreements
# ------------------------------------------

# Setup exchange
mrsal.setup_exchange(exchange=EXCHANGE,
                    exchange_type=EXCHANGE_TYPE)
# ------------------------------------------

# Setup queue for berlin agreements
mrsal.setup_queue(queue=QUEUE_1)


# Bind queue to exchange with binding key
mrsal.setup_queue_binding(exchange=EXCHANGE,
                        routing_key=BINDING_KEY_1,
                        queue=QUEUE_1)
# ----------------------------------

# Setup queue for september agreements
mrsal.setup_queue(queue=QUEUE_2)

# Bind queue to exchange with binding key
mrsal.setup_queue_binding(exchange=EXCHANGE,
                        routing_key=BINDING_KEY_2,
                        queue=QUEUE_2)
# ----------------------------------

# Publisher:

# Message ("uuid1") is published to the exchange will be routed to queue1
message1 = 'uuid1'
prop1 = pika.BasicProperties(
        app_id='test_exchange_topic',
        message_id='berlin',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key=ROUTING_KEY_1,
                        message=json.dumps(message1),
                        prop=prop1)

# Message ("uuid2") is published to the exchange will be routed to queue2
message2 = 'uuid2'
prop2 = pika.BasicProperties(
        app_id='test_exchange_topic',
        message_id='september',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key=ROUTING_KEY_2,
                        message=json.dumps(message2),
                        prop=prop2)
# ------------------------------------------

# Start consumer for every queue
mrsal.start_consumer(
    queue=QUEUE_1,
    callback=consumer_callback,
    callback_args=('localhost', QUEUE_1),
    inactivity_timeout=1,
    requeue=False
)

mrsal.start_consumer(
    queue=QUEUE_2,
    callback=consumer_callback,
    callback_args=('localhost', QUEUE_2),
    inactivity_timeout=1,
    requeue=False
)

```
<div id='fanoutExchange'/>

3. **Fanout Exchange**

    - A _fanout_ exchange duplicates and routes a received message to any associated queues, **_regardless_ of routing keys or pattern matching**. 
    - Fanout exchanges are useful when the same message needs to be passed to one or perhaps more queues with consumers who may process the message differently. 
    - Here, your provided keys will be entirely **ignored**. 

```py
EXCHANGE: str = 'agreements' 
EXCHANGE_TYPE: str = 'fanout' 

# In this case you don't need binding key to bound queue to exchange
# Messages is published with routing key equals to empty string because it will be ignored
ROUTING_KEY: str = ''

# Setup exchange
mrsal.setup_exchange(exchange=EXCHANGE,
                    exchange_type=EXCHANGE_TYPE)
```
<div id='headersExchange'/>

4. **Headers Exchange**

    - A headers RabbitMQ exchange type is a message routing system that uses `arguments` with `headers` and optional values to route messages. 
    - Header exchanges are identical to topic exchanges, except that instead of using routing keys, messages are routed based on header values. 
    - If the value of the header equals the value of supply during binding, the message matches. 
    - In the binding between exchange and queue, a specific argument termed `'x-match'` indicates whether all headers must match or only one. 
    - The `'x-match'` property has two possible values: `'any'` and `'all'` with `'all'` being the default. 
    - A value of `'all'` indicates that all header pairs (key, value) must match, whereas `'any'` indicates that at least one pair must match. 

<p align="center">
<img src="./doc_images/exchange_headers_wf.png" alt="drawing" width="600"/>
</p>

```py
def consumer_callback(host_param: str, queue_param: str, message_param: str):
    return True

EXCHANGE: str = 'agreements'
EXCHANGE_TYPE: str = 'headers'

QUEUE_1: str = 'zip_report'
Q1_ARGS = {'x-match': 'all', 'format': 'zip', 'type': 'report'}

QUEUE_2: str = 'pdf_report'
Q2_ARGS = {'x-match': 'any', 'format': 'pdf', 'type': 'log'}

HEADERS1 = {'format': 'zip', 'type': 'report'}
HEADERS2 = {'format': 'pdf', 'date': '2022'}
# ------------------------------------------

# Setup exchange
mrsal.setup_exchange(exchange=EXCHANGE,
                    exchange_type=EXCHANGE_TYPE)
# ------------------------------------------

# Setup queue
mrsal.setup_queue(queue=QUEUE_1)

# Bind queue to exchange with arguments
mrsal.setup_queue_binding(exchange=EXCHANGE,
                        queue=QUEUE_1,
                        arguments=Q1_ARGS)
# ------------------------------------------

# Setup queue
mrsal.setup_queue(queue=QUEUE_2)

# Bind queue to exchange with arguments
mrsal.setup_queue_binding(exchange=EXCHANGE,
                        queue=QUEUE_2,
                        arguments=Q2_ARGS)
# ------------------------------------------

# Publisher:
# Message ("uuid1") is published to the exchange with a set of headers
prop1 = pika.BasicProperties(
        app_id='test_exchange_headers',
        message_id='zip_report',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'format': 'zip', 'type': 'report'})
message1 = 'uuid1'
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key='',
                        message=json.dumps(message1),
                        prop=prop1)

# Message ("uuid2") is published to the exchange with a set of headers
prop2 = pika.BasicProperties(
        app_id='test_exchange_headers',
        message_id='pdf_date',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'format': 'pdf', 'date': '2022'})
message2 = 'uuid2'
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key='',
                        message=json.dumps(message2),
                        prop=prop2)
# ------------------------------------------

# Start consumer for every queue
mrsal.start_consumer(
    queue=QUEUE_1,
    callback=consumer_callback,
    callback_args=('localhost', QUEUE_1),
    inactivity_timeout=2,
    requeue=False
)

mrsal.start_consumer(
    queue=QUEUE_2,
    callback=consumer_callback,
    callback_args=('localhost', QUEUE_2),
    inactivity_timeout=2,
    requeue=False
)
```
<div id='delayExchange'/>

5. **Delay Exchange**
    - A message which reaches to exchange from a publisher, will be instantaneously delivered to the bound queue. 
    - But if you want to add delay to the delivery time for the message from exchange to queue, then you can use delay exchange.
    - A user can declare an **exchange** with: 
        - The type `x-delayed-message` and 
        - Arguments with the kye `x-delayed-type` to specify how the messages will be routed after the delay period specified.
    - Then **publish** messages with the header `x-delay` expressing in milliseconds a delay time for the message. 
    - The message will be delivered to the respective queues after `x-delay` milliseconds.
    - **NB** This plugin has known limitations: for more info check here https://github.com/rabbitmq/rabbitmq-delayed-message-exchange#limitations

```py
def consumer_callback(host: str, queue: str, message: str):
    return True

# Setup exchange with delay message type
mrsal.setup_exchange(exchange='agreements',
                    exchange_type='x-delayed-message',
                    arguments={'x-delayed-type': 'direct'})

# Setup queue
mrsal.setup_queue(queue='agreements_queue')                    

# Bind queue to exchange with routing_key
qb_result: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                        routing_key='agreements_key',
                                                        queue='agreements_queue')

"""
Publisher:
    Message ("uuid1") is published with x-delay=3000
    Message ("uuid2") is published with x-delay=1000
"""
x_delay1: int = 3000
message1 = 'uuid1'
prop1 = pika.BasicProperties(
        app_id='test_exchange_delay_letters',
        message_id='uuid1_3000ms',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': x_delay1})
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message1),
                        prop=prop1)

x_delay2: int = 1000
message2 = 'uuid2'
prop2 = pika.BasicProperties(
        app_id='test_exchange_delay_letters',
        message_id='uuid2_1000ms',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': x_delay2})
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message2),
                        prop=prop2)


"""
Consumer from main queue
    Message ("uuid2"): Consumed first because its delivered from exchange to the queue
    after x-delay=1000ms which is the shortest time.
    Message ("uuid1"): Consumed at second place because its x-delay = 3000 ms.
"""
mrsal.start_consumer(
    queue='agreements_queue',
    callback=consumer_callback,
    callback_args=('localhost', 'agreements_queue'),
    inactivity_timeout=3,
    requeue=False
)
```
---
<div id='queueWithDeadLetters'/>

## Setup Queue With Dead Letters Exchange

Dead messages are:
- Some messages become undeliverable or unhandled even when received by the broker. 
- This can happen when: 
    - The amount of time the message has spent in a queue exceeds the time to live `TTL` (x-message-ttl). 
    - When a message is `negatively-acknowledged` by the consumer. 
    - When the queue reaches its capacity.
- Such a message is called a `dead message`.

```py
def consumer_callback(host: str, queue: str, message: str):
    if message == b'"\\"uuid3\\""':
        time.sleep(3)
    return message != b'"\\"uuid2\\""'

def consumer_dead_letters_callback(host_param: str, queue_param: str, message_param: str):
    return True
# ------------------------------------------    
# Setup dead letters exchange
mrsal.setup_exchange(exchange='dl_agreements',
                     exchange_type='direct')

# Setup main exchange
mrsal.setup_exchange(exchange='agreements',
                     exchange_type='direct')
# ------------------------------------------
# Setup main queue with arguments where we specify DL_EXCHANGE, DL_ROUTING_KEY and TTL
mrsal.setup_queue(queue='agreements_queue',
                    arguments={'x-dead-letter-exchange': 'dl_agreements',
                                'x-dead-letter-routing-key': 'dl_agreements_key',
                                'x-message-ttl': 2000})
mrsal.setup_queue(queue=queue,
                arguments=queue_args)

mrsal.setup_queue_binding(exchange='agreements',
                            routing_key='agreements_key',
                            queue='agreements_queue') 
# ------------------------------------------

# Bind DL_QUEUE to DL_EXCHANGE with DL_ROUTING_KEY
mrsal.setup_queue(queue='dl_agreements_queue')

mrsal.setup_queue_binding(exchange='dl_agreements',
                            routing_key='dl_agreements_key',
                            queue='dl_agreements_queue')
# ------------------------------------------

"""
Publisher:
    Message ("uuid1") is published
    Message ("uuid2") is published
    Message ("uuid3") is published
    Message ("uuid4") is published
"""
message1 = 'uuid1'
prop1 = pika.BasicProperties(
        app_id='test_exchange_dead_letters',
        message_id='msg_uuid1',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message1),
                        prop=prop1)

message2 = 'uuid2'
prop2 = pika.BasicProperties(
        app_id='test_exchange_dead_letters',
        message_id='msg_uuid2',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message2),
                        prop=prop2)

message3 = 'uuid3'
prop3 = pika.BasicProperties(
        app_id='test_exchange_dead_letters',
        message_id='msg_uuid3',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message3),
                        prop=prop3)

message4 = 'uuid4'
prop4 = pika.BasicProperties(
        app_id='test_exchange_dead_letters',
        message_id='msg_uuid4',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message4),
                        prop=prop4)                        

"""
Consumer from main queue
    Message ("uuid1"):
        - This message is positively-acknowledged by consumer.
        - Then it will be deleted from queue.
    Message ("uuid2"):
        - This message is rejected by consumer's callback.
        - Therefor it will be negatively-acknowledged by consumer.
        - Then it will be forwarded to dead-letters-exchange (x-first-death-reason: rejected).
    Message ("uuid3"):
        - This message has processing time in the consumer's callback equal to 3s
            which is greater that TTL=2s.
        - After processing will be positively-acknowledged by consumer.
        - Then it will be deleted from queue.
    Message ("uuid4"):
        - This message will be forwarded to dead-letters-exchange
            because it spent in the queue more than TTL=2s waiting "uuid3" to be processed
            (x-first-death-reason: expired).
"""
mrsal.start_consumer(
    queue='agreements_queue',
    callback=consumer_callback,
    callback_args=('localhost', 'agreements_queue'),
    inactivity_timeout=6,
    requeue=False
)
# ------------------------------------------                   
"""
Consumer from dead letters queue
    Message ("uuid2"):
        - This message is positively-acknowledged by consumer.
        - Then it will be deleted from dl-queue.
    Message ("uuid4"):
        - This message is positively-acknowledged by consumer.
        - Then it will be deleted from dl-queue.
"""
mrsal.start_consumer(
    queue='dl_agreements_queue',
    callback=consumer_dead_letters_callback,
    callback_args=('localhost', 'dl_agreements_queue'),
    inactivity_timeout=3,
    requeue=False
)     
                                                                                                       
```
---
<div id='deadAndDelayLetters'/>

## Dead and Delay Letters Workflow

<p align="center">
<img src="./doc_images/dead_and_delay_letters_wf.png" alt="drawing" width="600"/>
</p>

```py
def consumer_callback(host: str, queue: str, message: str):
    if message == b'"\\"uuid3\\""':
        time.sleep(3)
    return message != b'"\\"uuid2\\""'

def consumer_dead_letters_callback(host_param: str, queue_param: str, message_param: str):
    return True

# ------------------------------------------

# Setup dead letters exchange
mrsal.setup_exchange(exchange='dl_agreements',
                    exchange_type='direct')

# Setup main exchange with 'x-delayed-message' type
# and arguments where we specify how the messages will be routed after the delay period specified
mrsal.setup_exchange(exchange='agreements',
                    exchange_type='x-delayed-message',
                    arguments={'x-delayed-type': 'direct'})
# ------------------------------------------

# Setup main queue with arguments where we specify DL_EXCHANGE, DL_ROUTING_KEY and TTL
mrsal.setup_queue(queue='agreements_queue',
                    arguments={'x-dead-letter-exchange': 'dl_agreements',
                            'x-dead-letter-routing-key': 'dl_agreements_key',
                            'x-message-ttl': 2000})

# Bind main queue to the main exchange with routing_key
mrsal.setup_queue_binding(exchange='agreements',
                            routing_key='agreements_key',
                            queue='agreements_queue')
# ------------------------------------------

# Bind DL_QUEUE to DL_EXCHANGE with DL_ROUTING_KEY
mrsal.setup_queue(queue='dl_agreements_queue')

mrsal.setup_queue_binding(exchange='dl_agreements',
                            routing_key='dl_agreements_key',
                            queue='dl_agreements_queue')
# ------------------------------------------

"""
Publisher:
    Message ("uuid1") is published with x-delay=2000
    Message ("uuid2") is published with x-delay=1000
    Message ("uuid3") is published with x-delay=3000
    Message ("uuid4") is published with x-delay=4000
"""
x_delay1: int = 2000  # ms
message1 = 'uuid1'
prop1 = pika.BasicProperties(
        app_id='test_exchange_dead_and_delay_letters',
        message_id='uuid1_2000ms',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': x_delay1})
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message1),
                        prop=prop1)

x_delay2: int = 1000
message2 = 'uuid2'
prop2 = pika.BasicProperties(
        app_id='test_exchange_dead_and_delay_letters',
        message_id='uuid2_1000ms',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': x_delay2})
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message2),
                        prop=prop2)

x_delay3: int = 3000
message3 = 'uuid3'
prop3 = pika.BasicProperties(
        app_id='test_exchange_dead_and_delay_letters',
        message_id='uuid3_3000ms',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': x_delay3})
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message3),
                        prop=prop3)

x_delay4: int = 4000
message4 = 'uuid4'
prop4 = pika.BasicProperties(
        app_id='test_exchange_dead_and_delay_letters',
        message_id='uuid4_4000ms',
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': x_delay4})
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message4),
                        rop=prop4)
# ------------------------------------------

"""
Consumer from main queue
    Message ("uuid2"): Consumed first because its delivered from exchange to the queue
    after x-delay=1000ms which is the shortest time.
        - This message is rejected by consumer's callback.
        - Therefor it will be negatively-acknowledged by consumer.
        - Then it will be forwarded to dead-letters-exchange (x-first-death-reason: rejected).
    Message ("uuid1"): Consumed at second place because its x-delay = 2000 ms.
        - This message is positively-acknowledged by consumer.
        - Then it will be deleted from queue.
    Message ("uuid3"): Consumed at third place because its x-delay = 3000 ms.
        - This message has processing time in the consumer's callback equal to 3s
            which is greater that TTL=2s.
        - After processing will be positively-acknowledged by consumer.
        - Then it will be deleted from queue.
    Message ("uuid4"): Consumed at fourth place because its x-delay = 4000 ms.
        - This message will be forwarded to dead-letters-exchange
            because it spent in the queue more than TTL=2s waiting "uuid3" to be processed
            (x-first-death-reason: expired).
"""
mrsal.start_consumer(
    queue='agreements_queue',
    callback=consumer_callback,
    callback_args=('localhost', 'agreements_queue'),
    inactivity_timeout=6,
    requeue=False
)
# ------------------------------------------

"""
Consumer from dead letters queue
    Message ("uuid2"):
        - This message is positively-acknowledged by consumer.
        - Then it will be deleted from dl-queue.
    Message ("uuid4"):
        - This message is positively-acknowledged by consumer.
        - Then it will be deleted from dl-queue.
"""

mrsal.start_consumer(
    queue='dl_agreements_queue',
    callback=consumer_dead_letters_callback,
    callback_args=('localhost', 'dl_agreements_queue'),
    inactivity_timeout=3,
    requeue=False
)
```
---
<div id='redeliverLettersWithDelay'/>

## Redeliver Rejected Letters With Delay Workflow

It's possible to redeliver the rejected messages with delay time.

```py
import json
import time

import mrsal.config.config as config
import pika
import tests.config as test_config
from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)

mrsal = Mrsal(host=test_config.HOST,
              port=config.RABBITMQ_PORT,
              credentials=config.RABBITMQ_CREDENTIALS,
              virtual_host=config.V_HOST,
              verbose=True)
mrsal.connect_to_server()

def test_redelivery_with_delay():

    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange='agreements')
    mrsal.queue_delete(queue='agreements_queue')
    # ------------------------------------------
    queue_arguments = None
    # ------------------------------------------

    # Setup main exchange with delay type
    exch_result1: pika.frame.Method = mrsal.setup_exchange(exchange='agreements',
                                                           exchange_type='x-delayed-message',
                                                           arguments={'x-delayed-type': 'direct'})
    assert exch_result1 != None
    # ------------------------------------------

    # Setup main queue
    q_result1: pika.frame.Method = mrsal.setup_queue(queue='agreements_queue')
    assert q_result1 != None

    # Bind main queue to the main exchange with routing_key
    qb_result1: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                              routing_key='agreements_key',
                                                              queue='agreements_queue')
    assert qb_result1 != None
    # ------------------------------------------

    """
    Publisher:
      Message ("uuid1") is published with delay 1 sec
      Message ("uuid2") is published with delay 2 sec
    """
    message1 = 'uuid1'
    prop1 = pika.BasicProperties(
        app_id='test_delivery-limit',
        message_id='msg_uuid1',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': 1000, 'x-retry-limit': 2})
    mrsal.publish_message(exchange='agreements',
                          routing_key='agreements_key',
                          message=json.dumps(message1), prop=prop1)

    message2 = 'uuid2'
    prop2 = pika.BasicProperties(
        app_id='test_delivery-limit',
        message_id='msg_uuid2',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers={'x-delay': 2000, 'x-retry-limit': 3, 'x-retry': 0})
    mrsal.publish_message(exchange='agreements',
                          routing_key='agreements_key',
                          message=json.dumps(message2), prop=prop2)

    # ------------------------------------------
    # Waiting for the delay time of the messages in the exchange. Then will be delivered to the queue.
    time.sleep(3)

    # Confirm messages are published
    result: pika.frame.Method = mrsal.setup_queue(queue='agreements_queue', passive=True)
    message_count = result.method.message_count
    log.info(f'Message count in queue "agreements_queue" before consuming= {message_count}')
    assert message_count == 2

    log.info(f'===== Start consuming from "agreements_queue" ========')
    """
    Consumer from main queue
      Message ("uuid1"):
          - This message is positively-acknowledged by consumer.
          - Then it will be deleted from queue.
      Message ("uuid2"):
          - This message is rejected by consumer's callback.
          - Therefor it will be negatively-acknowledged by consumer.
          - Then it will be redelivered with incremented x-retry until, either is acknowledged or x-retry = x-retry-limit.
    """
    mrsal.start_consumer(
        queue='agreements_queue',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'agreements_queue'),
        inactivity_timeout=8,
        requeue=False,
        callback_with_delivery_info=True
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result: pika.frame.Method = mrsal.setup_queue(queue='agreements_queue', passive=True)
    message_count = result.method.message_count
    log.info(f'Message count in queue "agreements_queue" after consuming= {message_count}')
    assert message_count == 0

def consumer_callback(host: str, queue: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message: str):
    return message != b'"\\"uuid2\\""'


if __name__ == '__main__':
    test_redelivery_with_delay()

```
---
<div id='quorumWithDeliveryLimit'/>

## Quorum Queue With Delivery Limit Workflow

- The quorum queue is a modern queue type for RabbitMQ implementing a durable, replicated FIFO queue based on the Raft consensus algorithm. 
- It is available as of RabbitMQ 3.8.0.
- It is possible to set a delivery limit for a queue using a policy argument, delivery-limit.

For more info: [quorum-queues](https://www.rabbitmq.com/quorum-queues.html)

```py
import json
import time

import mrsal.config.config as config
import pika
import tests.config as test_config
from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)

mrsal = Mrsal(host=test_config.HOST,
              port=config.RABBITMQ_PORT,
              credentials=config.RABBITMQ_CREDENTIALS,
              virtual_host=config.V_HOST,
              verbose=True)
mrsal.connect_to_server()

def test_quorum_delivery_limit():

    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange='agreements')
    mrsal.queue_delete(queue='agreements_queue')
    # ------------------------------------------
    queue_arguments = {
        # Queue of quorum type
        'x-queue-type': 'quorum',
        # Set a delivery limit for a queue using a policy argument, delivery-limit.
        # When a message has been returned more times than the limit the message will be dropped 
        # or dead-lettered(if a DLX is configured).
        'x-delivery-limit': 3} 
    # ------------------------------------------

    # Setup main exchange
    exch_result1: pika.frame.Method = mrsal.setup_exchange(exchange='agreements',
                                                           exchange_type='direct')
    assert exch_result1 != None
    # ------------------------------------------

    # Setup main queue with arguments
    q_result1: pika.frame.Method = mrsal.setup_queue(queue='agreements_queue',
                                                     arguments=queue_arguments)
    assert q_result1 != None

    # Bind main queue to the main exchange with routing_key
    qb_result1: pika.frame.Method = mrsal.setup_queue_binding(exchange='agreements',
                                                              routing_key='agreements_key',
                                                              queue='agreements_queue')
    assert qb_result1 != None
    # ------------------------------------------

    """
    Publisher:
      Message ("uuid1") is published
      Message ("uuid2") is published
    """
    message1 = 'uuid1'
    prop1 = pika.BasicProperties(
        app_id='test_delivery-limit',
        message_id='msg_uuid1',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
    mrsal.publish_message(exchange='agreements',
                          routing_key='agreements_key',
                          message=json.dumps(message1), prop=prop1)

    message2 = 'uuid2'
    prop2 = pika.BasicProperties(
        app_id='test_delivery-limit',
        message_id='msg_uuid2',
        content_type=test_config.CONTENT_TYPE,
        content_encoding=test_config.CONTENT_ENCODING,
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)
    mrsal.publish_message(exchange='agreements',
                          routing_key='agreements_key',
                          message=json.dumps(message2), prop=prop2)

    # ------------------------------------------
    time.sleep(1)

    # Confirm messages are published
    result: pika.frame.Method = mrsal.setup_queue(queue='agreements_queue', passive=True,
                                                  arguments=queue_arguments)
    message_count = result.method.message_count
    log.info(f'Message count in queue "agreements_queue" before consuming= {message_count}')
    assert message_count == 2

    log.info(f'===== Start consuming from "agreements_queue" ========')
    """
    Consumer from main queue
      Message ("uuid1"):
          - This message is positively-acknowledged by consumer.
          - Then it will be deleted from queue.
      Message ("uuid2"):
          - This message is rejected by consumer's callback.
          - Therefor it will be negatively-acknowledged by consumer.
          - Then it will be redelivered until, either it's acknowledged or x-delivery-limit is reached.
    """
    mrsal.start_consumer(
        queue='agreements_queue',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'agreements_queue'),
        inactivity_timeout=1,
        requeue=True,
        callback_with_delivery_info=True
    )
    # ------------------------------------------

    # Confirm messages are consumed
    result: pika.frame.Method = mrsal.setup_queue(queue='agreements_queue', passive=True,
                                                  arguments=queue_arguments)
    message_count = result.method.message_count
    log.info(f'Message count in queue "agreements_queue" after consuming= {message_count}')
    assert message_count == 0

def consumer_callback(host: str, queue: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message: str):
    return message != b'"\\"uuid2\\""'

def consumer_dead_letters_callback(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    return True


if __name__ == '__main__':
    test_quorum_delivery_limit()

```
---
<div id='concurrentConsumers'/>

## Concurrent Consumers

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
from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)

mrsal = Mrsal(host=test_config.HOST,
              port=config.RABBITMQ_PORT,
              credentials=config.RABBITMQ_CREDENTIALS,
              virtual_host=config.V_HOST)
mrsal.connect_to_server()

APP_ID = "TEST_CONCURRENT_CONSUMERS"
EXCHANGE = "CLINIC"
EXCHANGE_TYPE = ExchangeType.direct
QUEUE_EMERGENCY = "EMERGENCY"
NUM_THREADS = 3
NUM_MESSAGES = 3
INACTIVITY_TIMEOUT = 3
ROUTING_KEY = "PROCESS FOR EMERGENCY"
MESSAGE_ID = "HOSPITAL_EMERGENCY"

def test_concurrent_consumer():
    # Delete existing queues and exchanges to use
    mrsal.exchange_delete(exchange=EXCHANGE)
    mrsal.queue_delete(queue=QUEUE_EMERGENCY)
    # ------------------------------------------
    # Setup exchange
    exch_result: pika.frame.Method = mrsal.setup_exchange(exchange=EXCHANGE,
                                                          exchange_type=EXCHANGE_TYPE)
    assert exch_result != None
    # ------------------------------------------
    # Setup queue for madrid agreements
    q_result: pika.frame.Method = mrsal.setup_queue(queue=QUEUE_EMERGENCY)
    assert q_result != None

    # Bind queue to exchange with binding key
    qb_result: pika.frame.Method = mrsal.setup_queue_binding(exchange=EXCHANGE,
                                                             routing_key=ROUTING_KEY,
                                                             queue=QUEUE_EMERGENCY)
    assert qb_result != None
    # ------------------------------------------
    # Publisher:
    # Publish NUM_MESSAGES to the queue
    for msg_index in range(NUM_MESSAGES):
        prop = pika.BasicProperties(
            app_id=APP_ID,
            message_id=MESSAGE_ID + str(msg_index),
            content_type=test_config.CONTENT_TYPE,
            content_encoding=test_config.CONTENT_ENCODING,
            delivery_mode=pika.DeliveryMode.Persistent,
            headers=None)
        message = "uuid_" + str(msg_index)
        mrsal.publish_message(exchange=EXCHANGE,
                              routing_key=ROUTING_KEY,
                              message=json.dumps(message), prop=prop)
    # ------------------------------------------
    time.sleep(1)
    # Confirm messages are routed to the queue
    result1 = mrsal.setup_queue(queue=QUEUE_EMERGENCY, passive=True)
    message_count1 = result1.method.message_count
    assert message_count1 == NUM_MESSAGES
    # ------------------------------------------
    # Start concurrent consumers
    start_time = time.time()
    mrsal.start_concurrence_consumer(total_threads=NUM_THREADS, queue=QUEUE_EMERGENCY,
                                     callback=consumer_callback_with_delivery_info,
                                     callback_args=(test_config.HOST, QUEUE_EMERGENCY),
                                     exchange=EXCHANGE, exchange_type=EXCHANGE_TYPE,
                                     routing_key=ROUTING_KEY,
                                     inactivity_timeout=INACTIVITY_TIMEOUT,
                                     callback_with_delivery_info=True)
    duration = time.time() - start_time
    log.info(f"Concurrent consumers are done in {duration} seconds")
    # ------------------------------------------
    # Confirm messages are consumed
    result2 = mrsal.setup_queue(queue=QUEUE_EMERGENCY, passive=True)
    message_count2 = result2.method.message_count
    assert message_count2 == 0

    mrsal.close_connection()

def consumer_callback_with_delivery_info(host_param: str, queue_param: str, method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str):
    time.sleep(5)
    return True
```
---
<div id='resources'/>

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
