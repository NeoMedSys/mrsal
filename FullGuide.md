# FULL GUIDE

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
RABBITMQ_DEFAULT_PASS = <password> 
RABBITMQ_DEFAULT_USER = <username>
RABBITMQ_DEFAULT_VHOST = <virtualHost>
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
    env_file:
      - '~/rabbitmq/rabbitmq.env'
    ports:
      - '5672:5672'
      - '15672:15672'
      - '5671:5671'
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
from mrsal import Mrsal

mrsal = Mrsal(host='localhost',
            port=5673,
            credentials=('root', 'password'),
            virtual_host='v_host')

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
- `headers`: Is useful when we want to send message with headers. E.g:
        - When exchange's type is `x-delayed-message` then we need to send messages to the exchange with `x-delay` header to specify delay time for message in exchange before route it to bound queue ([see example](#delayExchange)).
        - When exchange's type is `headers`, then we need to send messages with headers which match the binding-key of bound queues to the exchange ([see example](#headersExchange)).
```py
message: str = 'uuid'

# publish messages with  header x-delay expressing in milliseconds a delay time for the message.
headers={'x-delay': 2000}, 

mrsal.publish_message(fast_setup=False,
                exchange='agreements',
                routing_key='agreements_key',
                message=json.dumps(message),
                headers=headers
                )
```                        
--- 
<div id='startConsumer'/>

## Start Consumer

- Setup consumer:
    - Consumer start consuming the messages from the queue.
    - If `inactivity_timeout` is given the consumer will be canceled when inactivity_timeout is exceeded.
    - Send the consumed message to callback method to be processed, and then the message can be either:
        - Processed, then **correctly-acknowledge** and deleted from queue or 
        - Failed to process, **negatively-acknowledged** and then will be either
            - `Requeued` if requeue is True
            - `Dead letter` and deleted from queue if 
                - requeue is False
                - requeue is True and requeue attempt fails.

```py
def consumer_callback(host: str, queue: str, message: str):
    return True

QUEUE: str = 'agreements_queue'

mrsal.start_consumer(
        queue='agreements_queue',
        callback=consumer_callback,
        callback_args=(test_config.HOST, 'agreements_queue'),
        inactivity_timeout=6,
        requeue=False
    )
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
message2 = 'uuid2'
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key=ROUTING_KEY_2,
                        message=json.dumps(message2))

# Message ("uuid1") is published to the exchange and it's routed to queue1
message1 = 'uuid1'
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key=ROUTING_KEY_1,
                        message=json.dumps(message1))
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
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key=ROUTING_KEY_1,
                        message=json.dumps(message1))

# Message ("uuid2") is published to the exchange will be routed to queue2
message2 = 'uuid2'
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key=ROUTING_KEY_2,
                        message=json.dumps(message2))
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

message1 = 'uuid1'
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key='',
                        message=json.dumps(message1),
                        headers=HEADERS1)

# Message ("uuid2") is published to the exchange with a set of headers

message2 = 'uuid2'
mrsal.publish_message(
                        exchange=EXCHANGE,
                        routing_key='',
                        message=json.dumps(message2),
                        headers=HEADERS2)
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
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message1),
                        headers={'x-delay': x_delay1})

x_delay2: int = 1000
message2 = 'uuid2'
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message2),
                        headers={'x-delay': x_delay2})


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
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message1))

message2 = 'uuid2'
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message2))

message3 = 'uuid3'
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message3))

message4 = 'uuid4'
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message4))                        

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
                            'x-message-ttl': test_config.MESSAGE_TTL})

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
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message1),
                        headers={'x-delay': x_delay1})

x_delay2: int = 1000
message2 = 'uuid2'
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message2),
                        headers={'x-delay': x_delay2})

x_delay3: int = 3000
message3 = 'uuid3'
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message3),
                        headers={'x-delay': x_delay3})

x_delay4: int = 4000
message4 = 'uuid4'
mrsal.publish_message(exchange='agreements',
                        routing_key='agreements_key',
                        message=json.dumps(message4),
                        headers={'x-delay': x_delay4})
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
<div id='resources'/>

## Resources

- [RabbitMQ Tutorials](https://www.rabbitmq.com/getstarted.html)
- [RabbitMQ Exchange Types: 6 Categories Explained Easy](https://hevodata.com/learn/rabbitmq-exchange-type/)
- [What is a Delayed Message Exchange?](https://www.cloudamqp.com/blog/what-is-a-delayed-message-exchange-in-rabbitmq.html#:~:text=The%20RabbitMQ%20delayed%20exchange%20plugin,in%20milliseconds%20can%20be%20specified.)
- [RabbitMQ Delayed Message Plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)
- [When and how to use the RabbitMQ Dead Letter Exchange](https://www.cloudamqp.com/blog/when-and-how-to-use-the-rabbitmq-dead-letter-exchange.html)
- [What is a RabbitMQ vhost?](https://www.cloudamqp.com/blog/what-is-a-rabbitmq-vhost.html)
- [Message Brokers](https://www.ibm.com/cloud/learn/message-brokers)
---
