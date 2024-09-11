import pika
from pydantic.dataclasses import dataclass
from mrsal.mrsal import Mrsal


@dataclass
class MrsalBlockingAMQP(Mrsal):
    use_blocking: bool = False

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
