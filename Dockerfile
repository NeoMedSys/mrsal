FROM rabbitmq:3.9.21-management

RUN apt-get update && \
    apt-get install -y curl unzip


# Add RabbitMQ Delayed Message Plugin
# Resource: Check limitaions https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
RUN curl -L https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/3.9.0/rabbitmq_delayed_message_exchange-3.9.0.ez > $RABBITMQ_HOME/plugins/rabbitmq_delayed_message_exchange-3.9.0.ez && \
    chown rabbitmq:rabbitmq $RABBITMQ_HOME/plugins/rabbitmq_delayed_message_exchange-3.9.0.ez

RUN rabbitmq-plugins enable rabbitmq_delayed_message_exchange

# You can disable this plugin by calling 
# rabbitmq-plugins disable rabbitmq_delayed_message_exchange 
# PS!! but note that ALL DELAYED MESSAGES THAT HAVEN'T BEEN DELIVERED WILL BE LOST.