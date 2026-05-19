"""DLX path with retry cycles against a real broker.

Closes the gap behind Issue 1b: with ``dlx_routing_key`` set, the bound DLX
routing key must be the one actually used at publish time, otherwise the
parked message lands somewhere no consumer is listening.

We force the terminal DLX path by setting ``max_retry_time_limit=0`` — the
first failure is treated as retry-exhausted and goes straight to the
``.dlx`` queue. That avoids waiting on the minute-grained ``x-message-ttl``
of the ``.retry`` queue, which would make the test impractically slow.
"""
import json
import threading
import time

import pytest

from mrsal import config
from mrsal.amqp.subclass import MrsalBlockingAMQP

from tests.integration.conftest import SyncConsumerRunner, broker_setup_args, raw_pika_channel


@pytest.mark.integration
def test_failed_callback_routes_to_dlx_with_custom_routing_key(
    unique_suffix, cleanup_topology,
):
    exchange = f"mrsal.it.dlx.{unique_suffix}"
    queue = f"mrsal.it.dlx.{unique_suffix}.q"
    routing_key = f"mrsal.it.dlx.{unique_suffix}.rk"
    dlx_routing_key = f"mrsal.it.dlx.{unique_suffix}.custom"

    cleanup_topology.exchange(exchange)
    cleanup_topology.exchange(f"{exchange}{config.DLX_SUFFIX}")
    cleanup_topology.queue(queue)
    cleanup_topology.queue(f"{queue}{config.DLX_SUFFIX}")
    cleanup_topology.queue(f"{queue}{config.RETRY_SUFFIX}")

    callback_done = threading.Event()

    consumer = MrsalBlockingAMQP(**broker_setup_args())
    runner = SyncConsumerRunner(consumer)

    def failing_callback(method_frame, properties, body):
        callback_done.set()
        raise RuntimeError("boom — deliberate failure to drive the DLX path")

    # max_retry_time_limit=0 short-circuits the cycle: the very first failure
    # is treated as retry-exhausted, so the message routes directly to the
    # terminal .dlx queue with the configured ``dlx_routing_key`` (instead of
    # the .retry binding).
    runner.start(
        queue_name=queue,
        exchange_name=exchange,
        exchange_type="direct",
        routing_key=routing_key,
        callback=failing_callback,
        auto_ack=False,
        dlx_enable=True,
        dlx_routing_key=dlx_routing_key,
        enable_retry_cycles=True,
        max_retry_time_limit=0,
        retry_cycle_interval=1,
        use_quorum_queues=False,
        inactivity_timeout=1,
    )

    publisher = MrsalBlockingAMQP(**broker_setup_args())
    payload = json.dumps({"id": 1, "msg": "to be DLX'd"}).encode("utf-8")
    try:
        runner.wait_ready()

        publisher.publish_message(
            exchange_name=exchange,
            routing_key=routing_key,
            message=payload,
            exchange_type="direct",
            queue_name=queue,
            auto_declare=False,
        )

        assert callback_done.wait(timeout=10), "Failing callback was never invoked"

        # Poll the terminal .dlx queue. basic_get is single-shot, so we retry
        # briefly to absorb the lag between the consumer ack'ing the original
        # and the DLX publish becoming visible. One channel held open for the
        # whole poll — opening a fresh connection per attempt would burn a
        # handshake every 100ms.
        dlx_queue = f"{queue}{config.DLX_SUFFIX}"
        method = props = body = None
        with raw_pika_channel() as ch:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                method, props, body = ch.basic_get(queue=dlx_queue, auto_ack=True)
                if method is not None:
                    break
                time.sleep(0.1)

        assert method is not None, f"No message arrived in {dlx_queue} within 10s"
        assert body == payload, "DLX payload should match the original publish"
        # The routing key used at publish time is what surfaces on the
        # delivered message — must equal the configured dlx_routing_key
        # (regression guard for Issue 1b).
        assert method.routing_key == dlx_routing_key, (
            f"Expected DLX routing_key={dlx_routing_key!r}, got {method.routing_key!r}"
        )

        headers = props.headers or {}
        assert headers.get("x-retry-exhausted") is True, (
            f"Terminal DLX message should be marked exhausted; headers={headers!r}"
        )
        assert "x-processing-error" in headers, (
            f"DLX message should carry the processing error reason; headers={headers!r}"
        )
        assert headers.get("x-cycle-count") == 1, (
            f"Cycle count should increment to 1 on first failure; headers={headers!r}"
        )
    finally:
        runner.stop()
        publisher.close()
        consumer.close()

    runner.raise_if_thread_errored()
