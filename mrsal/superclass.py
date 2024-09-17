# external
from functools import partial
import os
import ssl
from ssl import SSLContext
from typing import Any, Type
from mrsal.exceptions import MrsalSetupError
from pydantic.dataclasses import dataclass
from neolibrary.monitoring.logger import NeoLogger
from pydantic.deprecated.tools import json

# internal
from mrsal import config

log = NeoLogger(__name__, rotate_days=config.LOG_DAYS)


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
        :param int prefetch_count: Specifies a prefetch window in terms of whole messages.
        :param bool ssl: Set this flag to true if you want to connect externally to the rabbit server.
    """

    host: str
    port: int
    credentials: tuple[str, str]
    virtual_host: str
    ssl: bool = False
    verbose: bool = False
    prefetch_count: int = 5
    heartbeat: int = 60  # sec
    _connection = None
    _channel = None
    log = NeoLogger(__name__, rotate_days=config.LOG_DAYS)

    def __post_init__(self) -> None:
        if self.ssl:
            tls_dict = {
                    'crt': os.environ.get('RABBITMQ_CERT'),
                    'key': os.environ.get('RABBITMQ_KEY'),
                    'ca': os.environ.get('RABBITMQ_CAFILE')
                    }
            # empty string handling
            self.tls_dict = {cert: (env_var if env_var != '' else None) for cert, env_var in tls_dict.items()}
            config.ValidateTLS(**self.tls_dict)

    def _setup_exchange_and_queue(self, 
                                 exchange_name: str, queue_name: str, exchange_type: str,
                                 routing_key: str, exch_args: dict[str, str] | None = None,
                                 queue_args: dict[str, str] | None = None,
                                 bind_args: dict[str, str] | None = None,
                                 exch_durable: bool = True, queue_durable: bool =True,
                                 passive: bool = False, internal: bool = False,
                                 auto_delete: bool = False, exclusive: bool = False
                                 ) -> None:

        declare_exhange_dict = {
                'exchange': exchange_name,
                'exchange_type': exchange_type,
                'arguments': exch_args,
                'durable': exch_durable,
                'passive': passive,
                'internal': internal,
                'auto_delete': auto_delete
                }

        declare_queue_dict = {
                'queue': queue_name,
                'arguments': queue_args,
                'durable': queue_durable,
                'passive': passive,
                'exclusive': exclusive,
                'auto_delete': auto_delete
                }

        declare_queue_binding_dict = {
                'exchange': exchange_name,
                'queue': queue_name,
                'routing_key': routing_key,
                'arguments': bind_args

                }
        try:
            self._declare_exchange(**declare_exhange_dict)
            self._declare_queue(**declare_queue_dict)
            self._declare_queue_binding(**declare_queue_binding_dict)
            self.auto_declare_ok = True
        except MrsalSetupError:
            self.auto_declare_ok = False

    def on_connection_error(self, connection, exception):
        """
        Handle connection errors.
        """
        self.log.error(f"I failed to establish async connection: {exception}")
        try:
            if connection and connection.is_open:
                connection.close()
        except Exception as e:
            self.log.error(f'Oh lordy lord! I failed closing the connection with: {e}')

    def on_channel_open(
            self, exchange_name: str, queue_name: str,
            exchange_type: str, routing_key: str
            ) -> None:
        """
        Open a channel once the connection is established.
        """
        if self._connection and self._connection:
            self._channel = self._connection.channel()
            self._channel.basic_qos(prefetch_count=self.prefetch_count)
            self.log.info(f"Channel opened with prefetch count: {self.prefetch_count}")
            self._setup_exchange_and_queue(
                    exchange_name=exchange_name, queue_name=queue_name,
                    exchange_type=exchange_type, routing_key=routing_key
                    )
        else:
            self.log.error("Splæt! Connection is not open. Cannot create a channel.")

    def open_channel(self, exchange_name, queue_name, exchange_type, routing_key):
        """Open a channel once the connection is established."""
        self._connection.channel(
                on_open_callback=partial(
                    self.on_channel_open,
                    exchange_name=exchange_name, queue_name=queue_name,
                    exchange_type=exchange_type, routing_key=routing_key
                    )
                )

    def on_connection_open(self, 
                           connection,
                           exchange_name: str, queue_name: str,
                           exchange_type: str, routing_key: str
                           ) -> None:
        """
        Callback when the async connection is successfully opened.
        """
        self._connection = connection
        self.open_channel(exchange_name, queue_name, exchange_type, routing_key)

    def _declare_exchange(self, 
                             exchange: str, exchange_type: str,
                             arguments: dict[str, str] | None,
                             durable: bool, passive: bool,
                             internal: bool, auto_delete: bool
                            ) -> None:
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
            self._channel.exchange_declare(
                exchange=exchange, exchange_type=exchange_type,
                arguments=arguments, durable=durable,
                passive=passive, internal=internal,
                auto_delete=auto_delete
                )
        except Exception as e:
            raise MrsalSetupError(f'Oooopise! I failed declaring the exchange with : {e}')
        if self.verbose:
            self.log.success("Exchange declared yo!")

    def _declare_queue(self,
                    queue: str, arguments: dict[str, str] | None,
                    durable: bool, exclusive: bool,
                    auto_delete: bool, passive: bool
                    ) -> None:
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
            self._channel.queue_declare(queue=queue, arguments=arguments, durable=durable, exclusive=exclusive, auto_delete=auto_delete, passive=passive)
        except Exception as e:
            raise MrsalSetupError(f'Oooopise! I failed declaring the queue with : {e}')
        if self.verbose:
            self.log.info(f"Queue declared yo")

    def _declare_queue_binding(self, 
                            exchange: str, queue: str,
                            routing_key: str | None,
                            arguments: dict[str, str] | None
                            ) -> None:
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

        try:
            self._channel.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key, arguments=arguments)
            if self.verbose:
                self.log.info(f"The queue is bound to exchange successfully: queue={queue}, exchange={exchange}, routing_key={routing_key}")
        except Exception as e:
            raise MrsalSetupError(f'I failed binding the queue with : {e}')
        if self.verbose:
            self.log.info(f"Queue bound yo")
    
    def _ssl_setup(self) -> SSLContext:
        """_ssl_setup is private method we are using to connect with rabbit server via signed certificates and some TLS settings.

        Parameters
        ----------

        Returns
        -------
        SSLContext

        """
        context = ssl.create_default_context(cafile=self.tls_dict['ca'])
        context.load_cert_chain(certfile=self.tls_dict['crt'], keyfile=self.tls_dict['key'])
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

