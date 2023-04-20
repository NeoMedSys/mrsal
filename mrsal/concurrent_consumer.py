import concurrent.futures
import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Tuple

from mrsal.config.logging import get_logger
from mrsal.mrsal import Mrsal

log = get_logger(__name__)

@dataclass
class ConcurrentConsumer:
    """
    """
    mrsal: Mrsal
    queue: str
    callback: Callable
    num_threads: int = 4
    callback_args: Tuple[str, Any] = None,
    requeue: bool = False
    exchange: str = None
    exchange_type: str = None
    routing_key: str = None
    fast_setup: bool = False
    inactivity_timeout: int = None
    callback_with_delivery_info: bool = False

    def _start_consumer(self, thread_num: int):
        try:
            log.info(f"Start consumer: thread_num={thread_num}")
            mrsal_obj = Mrsal(host=self.mrsal.host,
                              port=self.mrsal.port,
                              credentials=self.mrsal.credentials,
                              virtual_host=self.mrsal.virtual_host,
                              ssl=self.mrsal.ssl,
                              verbose=self.mrsal.verbose,
                              prefetch_count=self.mrsal.prefetch_count,
                              heartbeat=self.mrsal.heartbeat,
                              blocked_connection_timeout=self.mrsal.blocked_connection_timeout)
            mrsal_obj.connect_to_server()

            mrsal_obj.start_consumer(
                callback=self.callback,
                callback_args=self.callback_args,
                queue=self.queue,
                requeue=self.requeue,
                exchange=self.exchange,
                exchange_type=self.exchange_type,
                routing_key=self.routing_key,
                fast_setup=self.fast_setup,
                inactivity_timeout=self.inactivity_timeout,
                callback_with_delivery_info=self.callback_with_delivery_info
            )
            mrsal_obj.close_connection()
            log.info(f"End consumer: thread_num={thread_num}")
        except Exception as e:
            log.error(f"Failed to consumer: {e}")

    def start_concurrence_consumer(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            executor.map(self._start_consumer, range(self.num_threads))
