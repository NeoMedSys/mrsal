# MRSAL AMQP
[![Release](https://img.shields.io/badge/release-1.0.9-etalue.svg)](https://pypi.org/project/mrsal/) [![Python 3.10](https://img.shields.io/badge/python-3.10--3.11--3.12lue.svg)](https://www.python.org/downloads/release/python-3103/)[![Mrsal Workflow](https://github.com/NeoMedSys/mrsal/actions/workflows/mrsal.yaml/badge.svg?branch=main)](https://github.com/NeoMedSys/mrsal/actions/workflows/mrsal.yaml)


## Intro
Mrsal is a simple to use message broker abstraction on top of [RabbitMQ](https://www.rabbitmq.com/) and [Pika](https://pika.readthedocs.io/en/stable/index.html). The goal is to make Mrsal trivial to re-use in all services of a distributed system and to make the use of advanced message queing protocols easy and safe. No more big chunks of repetive code across your services or bespoke solutions to handle dead letters. 

###### Mrsal is Arabic for a small arrow and is used to describe something that performs a task with lightness and speed. 

## Quick Start guide

### 0. Requirements
1. RabbitMQ server up and running
2. python 3.10 >=
3. tested on linux only

### 1. Installing
First things first: 

```bash
poetry add mrsal
```

Next set the default username, password and servername for your RabbitMQ setup. It's advisable to use a `.env` script or `(.zsh)rc` file for persistence.

```bash
[RabbitEnvVars]
RABBITMQ_USER=******
RABBITMQ_PASSWORD=******
RABBITMQ_VHOST=******
RABBITMQ_DOMAIN=******
RABBITMQ_PORT=******

# FOR TLS
RABBITMQ_CAFILE=/path/to/file
RABBITMQ_CERT=/path/to/file
RABBITMQ_KEY=/path/to/file
```

###### Mrsal was first developed by NeoMedSys and the research group [CRAI](https://crai.no/) at the univeristy hospital of Oslo.

### 2. Setup and connect
- Example 1: Lets create a blocking connection on localhost with no TLS encryption

```python
from mrsal.amqp.subclass import MrsalAMQP
mrsal = MrsalAMQP(
    host=RABBITMQ_DOMAIN,  # Use a custom domain if you are using SSL e.g. mrsal.on-example.com
    port=int(RABBITMQ_PORT),
    credentials=(RABBITMQ_USER, RABBITMQ_PASSWORD),
    virtual_host=RABBITMQ_VHOST,
    ssl=False, # Set this to True for SSL/TLS (you will need to set the cert paths if you do so)
    use_blocking=True  # Set this to False if you want to start an async connection
)

# boom you are staged for connection. This instantiation stages for connection only
```

### 2 Publish
Now lets publish our message of friendship on the friendship exchange.
Note: When `auto_declare=True` means that MrsalAMQP will create the specified `exchange` and `queue`, then bind them together using `routing_key` in one go. If you want to customize each step then turn off auto_declare and specify each step yourself with custom arguments etc.

```python
# BasicProperties is used to set the message properties
prop = pika.BasicProperties(
        app_id='zoomer_app',
        message_id='zoomer_msg',
        content_type=' application/json',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent,
        headers=None)

message_body = {'zoomer_message': 'Get it yia bish'}

# Publish the message to the exchange to be routed to queue
mrsal.publish_message(exchange_name='zoomer_x',
                        exchange_type='direct',
                        queue_name='zoomer_q',
                        routing_key='zoomer_key',
                        message=message_body, 
                        prop=prop,
                        auto_declare=True)
```

### 3 Consume

Now lets setup a consumer that will listen to our very important messages. If you are using scripts rather than notebooks then it's advisable to run consume and publish separately. We are going to need a callback function which is triggered upon receiving the message from the queue we subscribe to. You can use the callback function to activate something in your system.

Note: 
- If you start a consumer with `callback_with_delivery_info=True` then your callback function should have at least these params `(method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str)`. 
- If not, then it should have at least `(message_param: str)`
- We can use pydantic BaseModel classes to enforce types in the body

```python
from pydantic import BaseModel

class ZoomerNRJ(BaseModel):
    zoomer_message: str

def consumer_callback_with_delivery_info(method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, body: str):
    if 'Get it' in body:
        app_id = properties.app_id
        msg_id = properties.message_id
        print(f'app_id={app_id}, msg_id={msg_id}')
        print('Slay with main character vibe')
    else:
        raise SadZoomerEnergyError('Zoomer sad now')

mrsal.start_consumer(
        queue_name='zoomer_q',
        exchange_name='zoomer_x',
        callback_args=None,  # no need to specifiy if you do not need it
        callback=consumer_callback_with_delivery_info,
        auto_declare=False,
        auto_ack-False
    )
```

Done! Your first message of zommerism has been sent to the zoomer queue on the exchange of Zoomeru.

That simple! You have now setup a full advanced message queueing protocol that you can use to promote friendship or other necessary communication between your services.

###### Note! There are many parameters and settings that you can use to set up a more sophisticated communication protocol in both blocking or async connection with pydantic BaseModels to enforce data types in the expected payload.
---
## References

- [RabbitMQ Tutorials](https://www.rabbitmq.com/getstarted.html)
- [RabbitMQ Exchange Types: 6 Categories Explained Easy](https://hevodata.com/learn/rabbitmq-exchange-type/)
- [What is a Delayed Message Exchange?](https://www.cloudamqp.com/blog/what-is-delayed-message-exchange-in-rabbitmq.html#:~:text=The%20RabbitMQ%20delayed%20exchange%20plugin,in%20milliseconds%20can%20be%20specified.)
- [RabbitMQ Delayed Message Plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)
- [When and how to use the RabbitMQ Dead Letter Exchange](https://www.cloudamqp.com/blog/whennd-how-to-use-the-rabbitmq-dead-letter-exchange.html)
- [What is a RabbitMQ vhost?](https://www.cloudamqp.com/blog/what-is-rabbitmq-vhost.html)
- [Message Brokers](https://www.ibm.com/cloud/learn/messagerokers)
- [mrsal_icon](https://www.pngegg.com/en/png-mftic)
---
