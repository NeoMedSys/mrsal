import pika
from typing import Callable
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker, StreamLostError, ConnectionClosedByBroker
from pydantic.dataclasses import dataclass
from mrsal.mrsal import Mrsal
from neolibrary.monitoring.logger import NeoLogger
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_sleep_log

log = NeoLogger(__name__, log_days=10)

@dataclass
class MrsalBlockingAMQP(Mrsal):
    use_blocking: bool = False

    @retry(
        retry=retry_if_exception_type(
            AMQPConnectionError,
            ChannelClosedByBroker,
            ConnectionClosedByBroker,
            StreamLostError,
            ),
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        before_sleep=before_sleep_log(log, log.warning)
           )
    def setup_connection(self, context: dict[str, str] | None = None):
        """We can use setup_blocking_connection for establishing a connection to RabbitMQ server specifying connection parameters.
        The connection is blocking which is only advisable to use for the apps with low througput. 

        DISCLAIMER: If you expect a lot of traffic to the app or if its realtime then you should use async.

        Parameters
        ----------
        context : Dict[str, str]
            context is the structured map with information regarding the SSL options for connecting with rabbit server via TLS.
        """
        connection_info = f"""
                            Mrsal connection parameters:
                            host={self.host},
                            virtual_host={self.virtual_host},
                            port={self.port},
                            heartbeat={self.heartbeat},
                            ssl={self.ssl}
                            """
        if self.verbose:
            self.log.info(f"Establishing connection to RabbitMQ on {connection_info}")
        if self.ssl:
            self.log.info("Setting up TLS connection")
            context = self.__ssl_setup()
        ssl_options = pika.SSLOptions(context, self.host) if context else None
        credentials = pika.PlainCredentials(*self.credentials)
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    ssl_options=ssl_options,
                    virtual_host=self.virtual_host,
                    credentials=credentials,
                    heartbeat=self.heartbeat,
                    blocked_connection_timeout=self.blocked_connection_timeout,
                )
            )
            self._channel = self._connection.channel()
            # Note: prefetch is set to 1 here as an example only.
            # In production you will want to test with different prefetch values to find which one provides the best performance and usability for your solution.
            # use a high number of prefecth if you think the pods with Mrsal installed can handle it. A prefetch 4 will mean up to 4 async runs before ack is required
            self._channel.basic_qos(prefetch_count=self.prefetch_count)
            self.log.info(f"Connection established with RabbitMQ on {connection_info}")
        except (pika.exceptions.AMQPConnectionError, Exception) as err:
            self.log.error(f"I tried to connect with the RabbitMQ server but failed with: {err}")

    def setup_exchange_and_queue(self, 
                                 exchange_name: str, queue_name: str, exchange_type: str,
                                 routing_key: str, exch_args: dict[str, str] | None = None,
                                 queue_args: dict[str, str] | None = None,
                                 bind_args: dict[str, str] | None = None,
                                 exch_durable: bool = True, queue_durable: bool =True,
                                 passive: bool = False, internal: bool = False,
                                 auto_delete: bool = False, exclusive: bool = False
                                 ):

        self._declare_exchange(
                exchange=exchange_name,
                exchange_type=exchange_type,
                arguments=exch_args,
                durable=exch_durable,
                passive=passive,
                internal=internal,
                auto_delete=auto_delete
                )

        self._declare_queue(
                queue=queue_name,
                arguments=queue_args,
                durable=queue_durable,
                passive=passive,
                exclusive=exclusive,
                auto_delete=auto_delete
                )

        self._declare_queue_binding(
                exchange=exchange_name,
                queue=queue_name,
                routing_key=routing_key,
                arguments=bind_args
                )

    def start_consumer(self,
                       queue_name: str,
                       callback: Callable,
                       callback_args: dict[str, str | int | float | bool] | None = None,
                       auto_ack: bool = True,
                       inactivity_timeout: int = 5,
                       auto_declare: bool = True,
                       exchange_name: str | None = None,
                       exhange_type: str | None = None,
                       routing_key: str | None = None
                       ):
        """
        Start the consumer using blocking setup.
        :param queue: The queue to consume from.
        :param auto_ack: If True, messages are automatically acknowledged.
        :param inactivity_timeout: Timeout for inactivity in the consumer loop.
        :param callback: The callback function to process messages.
        :param callback_args: Optional arguments to pass to the callback.
        """
        if auto_declare:
            if None in (exchange_name, queue_name, exhange_type, routing_key):
                raise TypeError('Make sure that you are passing in all the necessary args for auto_declare')

            self.setup_exchange_and_queue(
                    exchange_name=exchange_name,
                    queue_name=queue_name,
                    exchange_type=exhange_type,
                    routing_key=routing_key
                    )
        try:
            for method_frame, properties, body in self._channel.consume(
                    queue=queue_name, auto_ack=auto_ack, inactivity_timeout=inactivity_timeout
                    ):
                if method_frame:
                    app_id = properties.app_id if properties else None
                    msg_id = properties.msg_id if properties else None

                    if self.verbose:
                        self.log.info(
                                """
                                Message received with:
                                - Method Frame: {method_frame)
                                - Redelivery: {method_frame.redelivered}
                                - Exchange: {method_frame.exchange}
                                - Routing Key: {method_frame.routing_key}
                                - Delivery Tag: {method_frame.delivery_tag}
                                - Properties: {properties}
                                """
                                )
                    if auto_ack:
                        self.log.success(f'I successfully received a message from: {app_id} with messageID: {msg_id}')
                    
                    if callback:
                        if callback_args:
                            callback(*callback_args, method_frame, properties, body)
                        else:

                            callback( method_frame, properties, body)
                else:
                    # continue consuming
                    continue
        except Exception as e:
            self.log.error(f'Oh lordy lord! I failed consuming ze messaj with: {e}')


    @dataclass
    class MrsalAsyncAMQP(Mrsal):
        # NOTE! fill in for async
        pass

