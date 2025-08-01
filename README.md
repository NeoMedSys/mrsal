# MRSAL AMQP
[![Release](https://img.shields.io/badge/release-2.1.1-blue.svg)](https://pypi.org/project/mrsal/) 
[![Python 3.10+](https://img.shields.io/badge/python-3.10%7C3.11%7C3.12-blue.svg)](https://www.python.org/downloads/)
[![Mrsal Workflow](https://github.com/NeoMedSys/mrsal/actions/workflows/mrsal.yaml/badge.svg?branch=main)](https://github.com/NeoMedSys/mrsal/actions/workflows/mrsal.yaml)
[![Coverage](https://neomedsys.github.io/mrsal/reports/badges/coverage-badge.svg)](https://neomedsys.github.io/mrsal/reports/coverage/htmlcov/)

## Intro
Mrsal is a **production-ready** message broker abstraction on top of [RabbitMQ](https://www.rabbitmq.com/), [aio-pika](https://aio-pika.readthedocs.io/en/latest/) and [Pika](https://pika.readthedocs.io/en/stable/index.html). 

**Why Mrsal?** Setting up robust AMQP in production is complex. You need dead letter exchanges, retry logic, quorum queues, proper error handling, queue management, and more. Mrsal gives you **enterprise-grade messaging** out of the box with just a few lines of code.

**What makes Mrsal production-ready:**

- **Intelligent Retry Logic**: Immediate retries + time-delayed retry cycles prevent message loss  
- **Dead Letter Exchange**: Automatic DLX setup with configurable retry cycles  
- **High Availability**: Quorum queues for data safety across cluster nodes  
- **Performance Tuning**: Queue limits, overflow behavior, lazy queues, prefetch control  
- **Zero Configuration**: Sensible defaults that work in production  
- **Full Observability**: Comprehensive logging and retry tracking  
- **Type Safety**: Pydantic integration for payload validation  
- **Async & Sync**: Both blocking and async implementations  

The goal is to make Mrsal **trivial to re-use** across all services in your distributed system and to make advanced message queuing protocols **easy and safe**. No more big chunks of repetitive code across your services or bespoke solutions to handle dead letters.

**Perfect for:**
- Microservices communication
- Event-driven architectures  
- Background job processing
- Real-time data pipelines
- Mission-critical message processing

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

Next set the default username, password and servername for your RabbitMQ setup. It's advisable to use a \`.env\` script or \`(.zsh)rc\` file for persistence.

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

###### Mrsal was first developed by NeoMedSys and the research group \[CRAI\](https://crai.no/) at the univeristy hospital of Oslo.

### 2. Setup and connect
- Example 1: Lets create a blocking connection on localhost with no TLS encryption

```python
from mrsal.amqp.subclass import MrsalBlockingAMQP

mrsal = MrsalBlockingAMQP(
    host=RABBITMQ_DOMAIN,  # Use a custom domain if you are using SSL e.g. mrsal.on-example.com
    port=int(RABBITMQ_PORT),
    credentials=(RABBITMQ_USER, RABBITMQ_PASSWORD),
    virtual_host=RABBITMQ_VHOST,
    ssl=False # Set this to True for SSL/TLS (you will need to set the cert paths if you do so)
)

# boom you are staged for connection. This instantiation stages for connection only
```

#### 2.1 Publish
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

#### 2.2 Consume

Now lets setup a consumer that will listen to our very important messages. If you are using scripts rather than notebooks then it's advisable to run consume and publish separately. We are going to need a callback function which is triggered upon receiving the message from the queue we subscribe to. You can use the callback function to activate something in your system.

Note:
- If you start a consumer with `callback_with_delivery_info=True` then your callback function should have at least these params `(method_frame: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, message_param: str)`.
- If not, then it should have at least `(message_param: str)`
- We can use pydantic BaseModel classes to enforce types in the body

```python
from pydantic import BaseModel

class ZoomerNRJ(BaseModel):
    zoomer_message: str

def consumer_callback_with_delivery_info(
     method_frame: pika.spec.Basic.Deliver,
     properties: pika.spec.BasicProperties,
     body: str
     ):
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
        auto_declare=True,
        auto_ack=False
    )
```

Done! Your first message of zommerism has been sent to the zoomer queue on the exchange of Zoomeru in a blocking connection. Lets see how we can do it in async in the next step.

### 3. Setup and Connect Async
Its usually the best practise to use async consumers if high throughput is expected. We can easily do this by adjusting the code a little bit to fit the framework of async connection in python.
```python
from mrsal.amqp.subclass import MrsalAsyncAMQP

mrsal = MrsalAsyncAMQP(
    host=RABBITMQ_DOMAIN,  # Use a custom domain if you are using SSL e.g. mrsal.on-example.com
    port=int(RABBITMQ_PORT),
    credentials=(RABBITMQ_USER, RABBITMQ_PASSWORD),
    virtual_host=RABBITMQ_VHOST,
    ssl=False # Set this to True for SSL/TLS (you will need to set the cert paths if you do so)
)

# boom you are staged for async connection.
```

#### 3.1 Consume
Lets go turbo and set up the consumer in async for efficient AMQP handling
```python
import asyncio
from pydantic import BaseModel

class ZoomerNRJ(BaseModel):
    zoomer_message: str

async def consumer_callback_with_delivery_info(
     method_frame: pika.spec.Basic.Deliver,
     properties: pika.spec.BasicProperties,
     body: str
     ):
    if 'Get it' in body:
        app_id = properties.app_id
        msg_id = properties.message_id
        print(f'app_id={app_id}, msg_id={msg_id}')
        print('Slay with main character vibe')
    else:
        raise SadZoomerEnergyError('Zoomer sad now')

asyncio.run(mrsal.start_consumer(
        queue_name='zoomer_q',
        exchange_name='zoomer_x',
        callback_args=None,  # no need to specifiy if you do not need it
        callback=consumer_callback_with_delivery_info,
        auto_declare=True,
        auto_ack=False
    ))
```

That simple! You have now setups for full advanced message queueing protocols that you can use to promote friendship or other necessary communication between your services in both blocking or async connections.

###### Note! There are many parameters and settings that you can use to set up a more sophisticated communication protocol in both blocking or async connection with pydantic BaseModels to enforce data types in the expected payload.

### 4. Advanced Features

#### 4.1 Dead Letter Exchange & Retry Logic with Cycles

Mrsal provides sophisticated retry mechanisms with both immediate retries and time-delayed retry cycles:

```python
mrsal = MrsalBlockingAMQP(
    host=RABBITMQ_DOMAIN,
    port=int(RABBITMQ_PORT),
    credentials=(RABBITMQ_USER, RABBITMQ_PASSWORD),
    virtual_host=RABBITMQ_VHOST,
    dlx_enable=True,        # Default: creates '<exchange_name>.dlx'
    max_retries=3           # Immediate retries before DLX
)

# Advanced retry configuration with cycles
mrsal.start_consumer(
    queue_name='critical_queue',
    exchange_name='critical_exchange',
    exchange_type='direct',
    routing_key='critical_key',
    callback=my_callback,
    auto_ack=False,                    # Required for retry logic
    dlx_enable=True,                   # Enable DLX for this queue
    dlx_exchange_name='custom_dlx',    # Optional: custom DLX name
    dlx_routing_key='dlx_key',         # Optional: custom DLX routing
    enable_retry_cycles=True,          # Enable time-delayed retry cycles
    retry_cycle_interval=10,           # Minutes between retry cycles
    max_retry_time_limit=60,           # Total minutes before permanent failure
    immediate_retry_delay=4,           # Seconds between immediate retries
    requeue=True                       # Enable requeuing for retries
)
```

**How the advanced retry logic works:**

1. **Immediate Retries**: Message fails → retry up to \`max_retries\` times with \`immediate_retry_delay\` seconds between attempts
2. **Retry Cycles**: After immediate retries exhausted → send to DLX with TTL for time-delayed retry
3. **Cycle Tracking**: Each cycle increments counters and tracks total elapsed time
4. **Permanent Failure**: After \`max_retry_time_limit\` exceeded → message stays in DLX for manual review

**Benefits:**
- Handles transient failures with immediate retries  
- Handles longer outages with time-delayed cycles  
- Prevents infinite retry loops  
- Full observability with retry tracking  
- Manual intervention capability for persistent failures

#### 4.2 Queue Management & Performance

Configure queues for optimal performance and resource management:

```python
mrsal.start_consumer(
    queue_name='high_performance_queue',
    exchange_name='perf_exchange',
    exchange_type='direct',
    routing_key='perf_key',
    callback=my_callback,
    
    # Queue limits and overflow behavior
    max_queue_length=10000,              # Max messages before overflow
    max_queue_length_bytes=None,         # Optional: max queue size in bytes
    queue_overflow="drop-head",          # "drop-head" or "reject-publish"
    
    # Performance settings
    single_active_consumer=False,        # Allow parallel processing
    lazy_queue=False,                    # Keep messages in RAM for speed
    use_quorum_queues=True,              # High availability
    
    # Memory optimization (for low-priority queues)
    lazy_queue=True,                     # Store messages on disk
    single_active_consumer=True          # Sequential processing
)
```

**Queue Configuration Options:**

- **\`max_queue_length\`**: Limit queue size to prevent memory issues
- **\`queue_overflow\`**: 
  - \`"drop-head"\`: Remove oldest messages when full
  - \`"reject-publish"\`: Reject new messages when full
- **\`single_active_consumer\`**: Only one consumer processes at a time (good for ordered processing)
- **\`lazy_queue\`**: Store messages on disk instead of RAM (memory efficient)
- **\`use_quorum_queues\`**: Enhanced durability and performance in clusters

#### 4.3 Quorum Queues

Quorum queues provide better data safety and performance for production environments:

```python
mrsal = MrsalBlockingAMQP(
    host=RABBITMQ_DOMAIN,
    port=int(RABBITMQ_PORT),
    credentials=(RABBITMQ_USER, RABBITMQ_PASSWORD),
    virtual_host=RABBITMQ_VHOST,
    use_quorum_queues=True  # Default: enables quorum queues
)

# Per-queue configuration
mrsal.start_consumer(
    queue_name='high_availability_queue',
    exchange_name='ha_exchange',
    exchange_type='direct',
    routing_key='ha_key',
    callback=my_callback,
    use_quorum_queues=True  # This queue will be highly available
)
```

**Benefits:**
- Better data replication across RabbitMQ cluster nodes  
- Improved performance under high load  
- Automatic leader election and failover  
- Works great in Kubernetes and bare metal deployments

#### 4.4 Production-Ready Example

```python
from mrsal.amqp.subclass import MrsalBlockingAMQP
from pydantic import BaseModel
import json

class OrderMessage(BaseModel):
    order_id: str
    customer_id: str
    amount: float

def process_order(method_frame, properties, body):
    try:
        order_data = json.loads(body)
        order = OrderMessage(**order_data)
        
        # Process the order
        print(f"Processing order {order.order_id} for customer {order.customer_id}")
        
        # Simulate processing that might fail
        if order.amount < 0:
            raise ValueError("Invalid order amount")
            
    except Exception as e:
        print(f"Order processing failed: {e}")
        raise  # This will trigger retry logic

# Production-ready setup with full retry cycles
mrsal = MrsalBlockingAMQP(
    host=RABBITMQ_DOMAIN,
    port=int(RABBITMQ_PORT),
    credentials=(RABBITMQ_USER, RABBITMQ_PASSWORD),
    virtual_host=RABBITMQ_VHOST,
    dlx_enable=True,         # Automatic DLX for failed orders
    max_retries=3,           # Immediate retries
    use_quorum_queues=True,  # High availability
    prefetch_count=10        # Process up to 10 messages concurrently
)

mrsal.start_consumer(
    queue_name='orders_queue',
    exchange_name='orders_exchange',
    exchange_type='direct',
    routing_key='new_order',
    callback=process_order,
    payload_model=OrderMessage,        # Automatic validation
    auto_ack=False,                    # Manual ack for reliability
    auto_declare=True,                 # Auto-create exchange/queue/DLX
    
    # Advanced retry configuration
    enable_retry_cycles=True,          # Enable retry cycles
    retry_cycle_interval=15,           # 15 minutes between cycles
    max_retry_time_limit=120,          # 2 hours total retry time
    immediate_retry_delay=5,           # 5 seconds between immediate retries
    
    # Queue performance settings
    max_queue_length=50000,            # Handle large order volumes
    queue_overflow="reject-publish",   # Reject when full (backpressure)
    single_active_consumer=False       # Parallel processing for speed
)
```

**Note!** There are many parameters and settings that you can use to set up a more sophisticated communication protocol in both blocking or async connection with pydantic BaseModels to enforce data types in the expected payload.

---
