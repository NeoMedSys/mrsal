# GET STARTED

PY-AMQP Is a _message broker_ based on [**RabbitMQ**](https://www.rabbitmq.com/) with [**Pika**](https://pika.readthedocs.io/en/stable/#) which accepts, stores, and forwards messages (tasks).

## RabbitMQ Message Concepts

- **Producer** Is a user application that sends messages. Messages are not published directly to a queue; instead, the producer sends messages to an exchange. 
- **Exchange** Is responsible for routing the messages to different queues using header attributes, bindings, and routing keys. 
- **Binding** A binding is a "connection" that you build between a queue and an exchange.
- **Routing Key** The routing key is a message attribute taken into account by the exchange when deciding how to route a message.
- **Queue** Is a buffer that receives and stores messages until the consumer receives them.
- **Consumer** Is a user application that receives and handles messages.

## RabbitMQ Message Cycle

<p align="center">
<img src="./doc_images/standard_wf.png" alt="drawing" width="600"/>
</p>

1. The **producer** publishes a message to an exchange.

2. The **exchange** routes the message into the queues bound to it depending on exchange type and routing key.

3. The messages stay in the **queue** until they are handled by a consumer.

4. The **consumer** handles the message.
---

## Connect To RabbitMQ Server

This tutorial assumes RabbitMQ is installed and running on localhost on the port (5673). In case you use a different host, port or credentials, connections settings would require adjusting.

    ```py
    HOST: str = 'localhost'
    V_HOST: str = 'v_host'
    PORT: int = 5673
    USER: str = 'root'
    PASSWORD: str = 'password'
    CREDENTIALS: Tuple[str, str] = (USER, PASSWORD)

    amqp = Amqp(host=HOST,
                port=PORT,
                credentials=CREDENTIALS,
                virtual_host=V_HOST)

    amqp.setup_connection()
    ```
---

## RabbitMQ Exchange Types

1. **Direct Exchange**

    - Uses a message _routing key_ to transport messages to queues. 
    - The _routing key_ is a message attribute that the _producer_ adds to the message header. 
    - You can consider the routing key to be an "address" that the exchange uses to determine how the message should be routed. 
    - A message is delivered to the queue with the _binding key_ that **exactly** matches the message’s _routing key_. 
    
    ```py
    EXCHANGE: str = 'agreements' 
    EXCHANGE_TYPE: str = 'direct' 
    QUEUE_1: str = 'agreements_berlin_queue' 
    QUEUE_2: str = 'agreements_madrid_queue' 

    # Messages will published with this routing key
    ROUTING_KEY_1: str = 'berlin agreements' 
    ROUTING_KEY_2: str = 'madrid agreements' 

    # Setup exchange
    amqp.setup_exchange(exchange=EXCHANGE,
                        exchange_type=EXCHANGE_TYPE)

    # Setup queue for berlin agreements
    amqp.setup_queue(queue=QUEUE_1)

    # Bind queue to exchange with binding key
    amqp.setup_queue_binding(exchange=EXCHANGE,
                             routing_key=ROUTING_KEY_1,
                             queue=QUEUE_1)
    #----------------------------------
    # Setup queue for madrid agreements
    amqp.setup_queue(queue=QUEUE_2)

    # Bind queue to exchange with binding key
    amqp.setup_queue_binding(exchange=EXCHANGE,
                             routing_key=ROUTING_KEY_2,
                             queue=QUEUE_2)
    #----------------------------------                            
    # Publisher:
    prop = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent)

    # Message ("uuid1") is published to the exchange and it's routed to queue1
    message1 = 'uuid1'
    amqp.publish_message(exchange=EXCHANGE,
                            routing_key=ROUTING_KEY_1,
                            message=json.dumps(message1),
                            properties=prop)

    # Message ("uuid2") is published to the exchange and it's routed to queue2
    message2 = 'uuid1'
    amqp.publish_message(exchange=EXCHANGE,
                            routing_key=ROUTING_KEY_2,
                            message=json.dumps(message2),
                            properties=prop)
    ```

2. **Topic Exchange**

    - Topic RabbitMQ exchange type sends messages to queues depending on wildcard matches between the _routing key_ and the queue binding's _routing pattern_. 
    - "*" (star) can substitute for exactly one word.
    - "#" (hash) can substitute for zero or more words.
    - The routing patterns may include an asterisk ("*") to match a word in a specified position of the routing key (for example, a routing pattern of "agreements.*.*.b.*" only matches routing keys with "agreements" as the first word and "b" as the fourth word).

    ```py
    EXCHANGE: str = 'agreements' 
    EXCHANGE_TYPE: str = 'topic' 
    QUEUE_1: str = 'berlin_agreements' 
    QUEUE_2: str = 'september_agreements' 
    ROUTING_KEY_1: str = 'agreements.eu.berlin.august.2022' # Messages will published with this routing key
    ROUTING_KEY_2: str = 'agreements.eu.madrid.september.2022' # Messages will published with this routing key
    BINDING_KEY_1: str = 'agreements.eu.berlin.#' # Berlin agreements
    BINDING_KEY_2: str = 'agreements.#' # All agreements
    BINDING_KEY_3: str = 'agreements.*.*.september' # Agreements of september

    # Setup exchange
    amqp.setup_exchange(exchange=EXCHANGE,
                        exchange_type=EXCHANGE_TYPE)
    # Setup queue for berlin agreements
    amqp.setup_queue(queue=QUEUE_1)

    # Bind queue to exchange with binding key
    amqp.setup_queue_binding(exchange=EXCHANGE,
                                routing_key=BINDING_KEY_1,
                                queue=QUEUE_1)
    #----------------------------------
    # Setup queue for september agreements
    amqp.setup_queue(queue=QUEUE_2)

    # Bind queue to exchange with binding key
    amqp.setup_queue_binding(exchange=EXCHANGE,
                                routing_key=BINDING_KEY_3,
                                queue=QUEUE_2)
    #----------------------------------                            
    # Publisher:
    prop = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent)

    # Message ("uuid1") is published to the exchange with a set of headers and will be routed to queue1
    message1 = 'uuid1'
    amqp.publish_message(exchange=EXCHANGE,
                            routing_key=ROUTING_KEY_1,
                            message=json.dumps(message1),
                            properties=prop)

    # Message ("uuid2") is published to the exchange with a set of headers and will be routed to queue2
    message2 = 'uuid1'
    amqp.publish_message(exchange=EXCHANGE,
                            routing_key=ROUTING_KEY_2,
                            message=json.dumps(message2),
                            properties=prop)
    ```

3. **Fanout Exchange**

    - A _fanout_ exchange, like direct and topic exchange, duplicates and routes a received message to any associated queues, **_regardless_ of routing keys or pattern matching**. 
    - Fanout exchanges are useful when the same message needs to be passed to one or perhaps more queues with consumers who may process the message differently. 
    - Here, your provided keys will be entirely **ignored**. 

    ```py
    EXCHANGE: str = 'agreements' 
    EXCHANGE_TYPE: str = 'fanout' 
    QUEUE_1: str = 'Queue_A' 
    QUEUE_1: str = 'Queue_B' 
    ROUTING_KEY: str = ''

    # Setup exchange
    amqp.setup_exchange(exchange=EXCHANGE,
                        exchange_type=EXCHANGE_TYPE)
    # Setup queue for berlin agreements
    amqp.setup_queue(queue=QUEUE_1)

    # Bind queue to exchange with binding key
    amqp.setup_queue_binding(exchange=EXCHANGE,
                                routing_key=ROUTING_KEY,
                                queue=QUEUE_1)
    #----------------------------------
    # Setup queue for september agreements
    amqp.setup_queue(queue=QUEUE_2)

    # Bind queue to exchange with binding key
    amqp.setup_queue_binding(exchange=EXCHANGE,
                                routing_key=ROUTING_KEY,
                                queue=QUEUE_2)
    #----------------------------------                            
    # Publisher:
    prop = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        delivery_mode=pika.DeliveryMode.Persistent)

    # Message ("uuid1") is published to the exchange with a set of headers and will be routed to queue1 and queue2
    message1 = 'uuid1'
    amqp.publish_message(exchange=EXCHANGE,
                            routing_key=ROUTING_KEY,
                            message=json.dumps(message1),
                            properties=prop)
    ```

4. **Headers Exchange**

    - A headers RabbitMQ exchange type is a message routing system that uses arguments with headers and optional values to route messages. Header exchanges are identical to topic exchanges, except that instead of using routing keys, messages are routed based on header values. If the value of the header equals the value of supply during binding, the message matches. 
    - In the binding between exchange and queue, a specific argument termed "x-match" indicates whether all headers must match or only one. 
    - The "x-match" property has two possible values: "any" and "all," with "all" being the default. A value of "all" indicates that all header pairs (key, value) must match, whereas "any" indicates that at least one pair must match. 

    ```py
    EXCHANGE: str ='agreements' 
    EXCHANGE_TYPE: str ='headers' 
    QUEUE: str ='zip_report' 
    ROUTING_KEY: str =None # It will not be used for routing messages.

    # Setup exchange
    amqp.setup_exchange(exchange=EXCHANGE,
                        exchange_type=EXCHANGE_TYPE)
    # Setup queue
    amqp.setup_queue(queue=QUEUE)

    # Bind queue to exchange with arguments
    arguments={'x-match': 'all', 'format': 'zip', 'type': 'report'}
    amqp.setup_queue_binding(exchange=EXCHANGE,
                                routing_key=ROUTING_KEY,
                                queue=QUEUE,
                                arguments=arguments)

    # Publisher:
    #   Message ("uuid1") is published to the exchange with a set of headers
    prop = pika.BasicProperties(
        content_type='text/plain',
        content_encoding='utf-8',
        headers = {'format': 'zip', 'type': 'report'},
        delivery_mode=pika.DeliveryMode.Persistent)

    message = 'uuid1'
    amqp.publish_message(exchange=EXCHANGE,
                            routing_key=ROUTING_KEY,
                            message=json.dumps(message),
                            properties=prop)
    ```
5. **Delay Exchange**
    - A message which reaches to exchange from a publisher, will be instantaneously delivered to the bound queue. But if you want to add delay to the delivery time for the message from exchange to queue, then you can use delay exchange.
    - A user can declare an exchange with: 
        - The type _x-delayed-message_ and 
        - Arguments with the kye _x-delayed-type_ to specify how the messages will be routed after the delay period specified.
    - Then publish messages with the custom header _x-delay_ expressing in milliseconds a delay time for the message. 
    - The message will be delivered to the respective queues after _x-delay_ milliseconds.
    - **NB** This plugin has known limitations: for more info check here https://github.com/rabbitmq/rabbitmq-delayed-message-exchange#limitations

    ```py
    EXCHANGE: str = 'agreements' 
    EXCHANGE_TYPE: str = 'x-delayed-message' 
    EXCHANGE_ARGS: Dict[str, str] = {'x-delayed-type': 'direct'}
    QUEUE: str = 'agreements_berlin_queue' 
    ROUTING_KEY: str = 'berlin agreements' # Messages will published with this routing key

    # Setup exchange with delay message type
    amqp.setup_exchange(exchange=EXCHANGE,
                        exchange_type=EXCHANGE_TYPE,
                        arguments=EXCHANGE_ARGS)

    # Setup queue for berlin agreements
    amqp.setup_queue(queue=QUEUE)

    # Bind queue to exchange with binding key
    amqp.setup_queue_binding(exchange=EXCHANGE,
                                routing_key=ROUTING_KEY,
                                queue=QUEUE)
    #----------------------------------       
    # Publisher:
        #   Message ("uuid1") is published to the exchange with headers where x-delay=3000 ms
        #   then the message will be delivered from exchange to the respective queues after three sec.
    x_delay: int = 3000
    message1 = 'uuid1'
    prop1 = pika.BasicProperties(headers={'x-delay': x_delay})
    amqp.publish_message(exchange=EXCHANGE,
                        routing_key=ROUTING_KEY,
                        message=json.dumps(message1),
                        properties=prop1)

    # Publisher:
        #   Message ("uuid2") is published to the exchange with headers where x-delay=1000 ms
        #   then the message will be delivered from exchange to the respective queues after one sec.
    x_delay2: int = 1000
    message2 = 'uuid2'
    prop2 = pika.BasicProperties(headers={'x-delay': x_delay2})
    amqp.publish_message(exchange=EXCHANGE,
                        routing_key=ROUTING_KEY,
                        message=json.dumps(message2),
                        properties=prop2)
    ```
---

## Time To Live (TTL)

- You can us _x-message-ttl_ in queue arguments to specify an amount of time in ms for the message in queue before it considered as dead. 

    ```py
    queue: str = 'queue'
    queue_args = {'x-message-ttl': 2000}

    amqp.setup_queue(queue=queue,
                    arguments=queue_args)
    ```
---

## Setup Queue With Dead Letters Exchange

- Dead messages are:
    - Some messages become undeliverable or unhandled even when received by the broker. 
    - This can happen when: 
        - The amount of time the message has spent in a queue exceeds the time to live 'TTL' (x-message-ttl). 
        - When a message is negatively-acknowledged by the consumer. 
        - When the queue reaches its capacity.
    - Such a message is called a dead message.

    ```py
    dl_exchange: str = 'dl_exchange' 
    dl_routing_key: str = 'dl_routing_key' 
    queue: str = 'queue'

    queue_args = {'x-dead-letter-exchange': dl_exchange,
                  'x-dead-letter-routing-key': dl_routing_key}
    amqp.setup_queue(queue=queue,
                    arguments=queue_args)
    ```
---

# Start Consumer

- Setup consumer:
    - Consumer start consuming the messages from the queue until _inactivity_timeout_ is exceeded (if given).
    - Send the consumed message to callback method to be processed, and then the message can be either:
        - Processed, then correctly-acknowledge and deleted from queue or 
        - Failed to process, negatively-acknowledged and then will be either
            - requeued if requeue is True, or
            - deleted from queue if requeue is False or
            - dead letter and deleted from queue if dead-letters' configuration is setup and:
                - requeue is False or 
                - requeue is True and requeue attempt fails.

    ```py
    amqp.start_consumer(queue, callback, callback_args,
                        inactivity_timeout, requeue)
    ```
---

## Resources:

- [RabbitMQ Tutorials](https://www.rabbitmq.com/getstarted.html)
- [RabbitMQ Exchange Types: 6 Categories Explained Easy](https://hevodata.com/learn/rabbitmq-exchange-type/)
- [What is a Delayed Message Exchange?](https://www.cloudamqp.com/blog/what-is-a-delayed-message-exchange-in-rabbitmq.html#:~:text=The%20RabbitMQ%20delayed%20exchange%20plugin,in%20milliseconds%20can%20be%20specified.)
- [When and how to use the RabbitMQ Dead Letter Exchange](https://www.cloudamqp.com/blog/when-and-how-to-use-the-rabbitmq-dead-letter-exchange.html)