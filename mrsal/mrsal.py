import os
import ssl
from typing import Any, Optional, Type
from mrsal.amqp.blocking import MrsalBlockingAMQP
import asyncio
from pydantic.dataclasses import dataclass
from pika.exceptions import ChannelClosedByBroker, ConnectionClosedByBroker
from neolibrary.monitoring.logger import NeoLogger
from config.exceptions import MissingTLSCerts
from pydantic.deprecated.tools import json


@dataclass
# NOTE! change the doc style to google or numpy
class Mrsal:
    """
    Mrsal creates a layer on top of Pika's core, providing methods to setup a RabbitMQ broker with multiple functionalities.

    Properties:
        :param str host: Hostname or IP Address to connect to
        :param int port: TCP port to connect to
        :param pika.credentials.Credentials credentials: auth credentials
        :param str virtual_host: RabbitMQ virtual host to use
        :param bool verbose: If True then more INFO logs will be printed
        :param int heartbeat: Controls RabbitMQ's server heartbeat timeout negotiation
            during connection tuning.
        :param int blocked_connection_timeout: blocked_connection_timeout
            is the timeout, in seconds,
            for the connection to remain blocked; if the timeout expires,
                the connection will be torn down
        :param int prefetch_count: Specifies a prefetch window in terms of whole messages.
        :param bool ssl: Set this flag to true if you want to connect externally to the rabbit server.
    """

    host: str
    port: str
    credentials: tuple[str, str]
    virtual_host: str
    use_blocking: bool
    ssl: bool = False
    verbose: bool = False
    prefetch_count: int = 1
    heartbeat: int = 600  # sec
    blocked_connection_timeout: int = 300  # sec
    _connection = None
    _channel = None
    log = NeoLogger(__name__, rotate_days=10)

    def __post_init__(self):
        if self.ssl:
            self.tls_crt = os.environ.get('RABBITMQ_CERT', 'yikes.crt')
            self.tls_key = os.environ.get('RABBITMQ_KEY', 'yikes.key')
            self.tls_ca = os.environ.get('RABBITMQ_CAFILE', 'yikes.ca')

            test_list_tuple = [('tls.crt', self.tls_crt), ('tls.key', self.tls_key), ('tls.ca', self.tls_ca)]
            yikes_matches = [tls for tls, yikes in test_list_tuple  if 'yikes' in yikes]

            if yikes_matches:
                raise MissingTLSCerts(f"TLS/SSL is activated but I could not find the following certs: {', '.join(yikes_matches)}")
        self.use_blocking = True if issubclass(self.__class__, MrsalBlockingAMQP) else False

    def _setup_exchange_and_queue(self, 
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

    def on_connection_error(self, _unused_connection, exception):
        """
        Handle connection errors.
        """
        self.log.error(f"I failed to establish async connection: {exception}")

    async def open_channel(self):
        """
        Open a channel once the connection is established.
        """
        self._channel = await self.conn.channel()
        await self._channel.basic_qos(prefetch_count=self.prefetch_count)

    def on_connection_open(self, connection):
        """
        Callback when the async connection is successfully opened.
        """
        self.conn = connection
        self.log.info("Async connection established.")
        asyncio.create_task(self.open_channel())

    async def _declare_exchange(self, 
                             exchange: str, exchange_type: str,
                             arguments: dict[str, str] | None,
                             durable: bool, passive: bool,
                             internal: bool, auto_delete: bool
                            ):
        """This method creates an exchange if it does not already exist, and if the exchange exists, verifies that it is of the correct and expected class.

        If passive set, the server will reply with Declare-Ok if the exchange already exists with the same name,
        and raise an error if not and if the exchange does not already exist, the server MUST raise a channel exception with reply code 404 (not found).

        :param str exchange: The exchange name
        :param str exchange_type: The exchange type to use
        :param bool passive: Perform a declare or just check to see if it exists
        :param bool durable: Survive a reboot of RabbitMQ
        :param bool auto_delete: Remove when no more queues are bound to it
        :param bool internal: Can only be published to by other exchanges
        :param dict arguments: Custom key/value pair arguments for the exchange
        :returns: Method frame from the Exchange.Declare-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type `spec.Exchange.DeclareOk`
        """
        exchange_declare_info = f"""
                                exchange={exchange},
                                exchange_type={exchange_type},
                                durable={durable},
                                passive={passive},
                                internal={internal},
                                auto_delete={auto_delete},
                                arguments={arguments}
                                """
        if self.verbose:
            self.log.info(f"Declaring exchange with: {exchange_declare_info}")
        try:
            if self.use_blocking:
                self._channel.exchange_declare(
                    exchange=exchange, exchange_type=exchange_type,
                    arguments=arguments, durable=durable,
                    passive=passive, internal=internal,
                    auto_delete=auto_delete
                    )
            else:
                await self._channel.exchange_declare(
                    exchange=exchange, exchange_type=exchange_type,
                    arguments=arguments, durable=durable,
                    passive=passive, internal=internal,
                    auto_delete=auto_delete
                    )

        except (TypeError, AttributeError, ChannelClosedByBroker, ConnectionClosedByBroker) as err:
                self.log.error(f"I tried to declare an exchange but failed with: {err}")
        if self.verbose:
            self.log.info(f"Exchange is declared successfully with blocking set to {self.use_blocking}: {exchange_declare_info}")

    def _declare_queue(self,
                    queue: str, arguments: dict[str, str] | None,
                    durable: bool, exclusive: bool,
                    auto_delete: bool, passive: bool
                    ):
        """Declare queue, create if needed. This method creates or checks a queue.
        When creating a new queue the client can specify various properties that control the durability of the queue and its contents,
        and the level of sharing for the queue.

        Use an empty string as the queue name for the broker to auto-generate one.
        Retrieve this auto-generated queue name from the returned `spec.Queue.DeclareOk` method frame.

        :param str queue: The queue name; if empty string, the broker will create a unique queue name
        :param bool passive: Only check to see if the queue exists and raise `ChannelClosed` if it doesn't
        :param bool durable: Survive reboots of the broker
        :param bool exclusive: Only allow access by the current connection
        :param bool auto_delete: Delete after consumer cancels or disconnects
        :param dict arguments: Custom key/value arguments for the queue
        :returns: Method frame from the Queue.Declare-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type `spec.Queue.DeclareOk`
        """
        queue_declare_info = f"""
                                queue={queue},
                                durable={durable},
                                exclusive={exclusive},
                                auto_delete={auto_delete},
                                arguments={arguments}
                                """
        if self.verbose:
            self.log.info(f"Declaring queue with: {queue_declare_info}")

        try:
            if self.use_blocking:
                self._channel.queue_declare(queue=queue, arguments=arguments, durable=durable, exclusive=exclusive, auto_delete=auto_delete, passive=passive)
            else:
                await self._channel.queue_declare(queue=queue, arguments=arguments, durable=durable, exclusive=exclusive, auto_delete=auto_delete, passive=passive)
        except Exception as e:
            self.log.error(f'Ooopsie boy: I failed declaring queue: {e}')

        if self.verbose:
            self.log.info(f"Queue is declared successfully: {queue_declare_info}")

    def _declare_queue_binding(self, 
                            exchange: str, queue: str,
                            routing_key: str | None,
                            arguments: dict[str, str] | None
                            ):
        """Bind queue to exchange.

        :param str queue: The queue to bind to the exchange
        :param str exchange: The source exchange to bind to
        :param str routing_key: The routing key to bind on
        :param dict arguments: Custom key/value pair arguments for the binding

        :returns: Method frame from the Queue.Bind-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type `spec.Queue.BindOk`
        """
        if self.verbose:
            self.log.info(f"Binding queue to exchange: queue={queue}, exchange={exchange}, routing_key={routing_key}")

        if self.use_blocking:
            self._channel.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key, arguments=arguments)
        else:
            await self._channel.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key, arguments=arguments)
        if self.verbose:
            self.log.info(f"The queue is bound to exchange successfully: queue={queue}, exchange={exchange}, routing_key={routing_key}")
    
    def _ssl_setup(self) -> dict[str, str]:
        """_ssl_setup is private method we are using to connect with rabbit server via signed certificates and some TLS settings.

        Parameters
        ----------

        Returns
        -------
        Dict[str, str]

        """
        context = ssl.create_default_context(cafile=self.tls_ca)
        context.load_cert_chain(certfile=self.tls_crt, keyfile=self.tls_key)
        return context

    def validate_payload(self, payload: Any, model: Type) -> None:
        """
        Parses and validates the incoming message payload using the provided dataclass model.
        :param payload: The message payload which could be of any type (str, bytes, dict, etc.).
        :param model: The pydantic dataclass model class to validate against.
        :return: An instance of the model if validation is successful, otherwise None.
        """
        # If payload is bytes, decode it to a string
        if isinstance(payload, bytes):
            payload = payload.decode('utf-8')

        # If payload is a string, attempt to load it as JSON
        if isinstance(payload, str):
            payload = json.loads(payload)  # Converts JSON string to a dictionary

        # Validate the payload against the provided model
        if isinstance(payload, dict):
            model(**payload)
        else:
            raise TypeError("Fool, we aint supporting this type yet {type(payload)}.. Bytes or str -- get it straight")
