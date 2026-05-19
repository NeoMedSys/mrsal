"""Async DLX path with retry cycles against a real broker.

Mirror of ``test_dlx_retry_cycle.py`` for ``MrsalAsyncAMQP``. The sync and
async DLX paths are separate implementations (``_publish_to_dlx_with_retry_cycle``
vs ``_async_publish_to_dlx_with_retry_cycle``) that share only the
``_build_dlx_retry_properties`` helper, so they can drift in subtle ways.
Asserting both end-to-end against a real broker is the cheapest way to keep
them honest.

Forces the terminal DLX path with ``max_retry_time_limit=0`` for the same
reason as the sync test — avoids waiting on the minute-grained ``.retry``
queue TTL.
"""
import asyncio
import json
import time

import pika
import pytest

from mrsal import config
from mrsal.amqp.subclass import MrsalAsyncAMQP

from tests.integration.conftest import AsyncConsumerRunner, broker_setup_args, raw_pika_channel


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_failed_callback_routes_to_dlx_with_custom_routing_key(
    unique_suffix, cleanup_topology,
):
    exchange = f"mrsal.it.adlx.{unique_suffix}"
    queue = f"mrsal.it.adlx.{unique_suffix}.q"
    routing_key = f"mrsal.it.adlx.{unique_suffix}.rk"
    dlx_routing_key = f"mrsal.it.adlx.{unique_suffix}.custom"

    cleanup_topology.exchange(exchange)
    cleanup_topology.exchange(f"{exchange}{config.DLX_SUFFIX}")
    cleanup_topology.queue(queue)
    cleanup_topology.queue(f"{queue}{config.DLX_SUFFIX}")
    cleanup_topology.queue(f"{queue}{config.RETRY_SUFFIX}")

    callback_done = asyncio.Event()

    consumer = MrsalAsyncAMQP(**broker_setup_args())
    runner = AsyncConsumerRunner(consumer)

    async def failing_callback(message, properties, body):
        callback_done.set()
        raise RuntimeError("boom — deliberate failure to drive the async DLX path")

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
    )

    try:
        await runner.wait_ready()

        payload = json.dumps({"id": 2, "msg": "async DLX"}).encode("utf-8")

        # Publish via sync pika in a worker thread — keeps this test simple
        # and matches the production pattern where async consumers often
        # receive from sync producers.
        def publish_one() -> None:
            with raw_pika_channel() as ch:
                ch.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=payload,
                    properties=pika.BasicProperties(delivery_mode=2),
                )

        await asyncio.to_thread(publish_one)
        await asyncio.wait_for(callback_done.wait(), timeout=10)

        # Poll the terminal .dlx queue for the DLX'd message. One pika channel
        # is held open in a worker thread for the whole poll — opening a fresh
        # connection per attempt would burn an AMQP handshake every 100ms.
        dlx_queue = f"{queue}{config.DLX_SUFFIX}"

        def poll_dlx():
            with raw_pika_channel() as ch:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    method, props, body = ch.basic_get(queue=dlx_queue, auto_ack=True)
                    if method is not None:
                        return method, props, body
                    time.sleep(0.1)
                return None, None, None

        method, props, body = await asyncio.to_thread(poll_dlx)

        assert method is not None, f"No message arrived in {dlx_queue} within 10s"
        assert body == payload, "Async DLX payload should match the original publish"
        assert method.routing_key == dlx_routing_key, (
            f"Expected async DLX routing_key={dlx_routing_key!r}, got {method.routing_key!r}"
        )

        headers = props.headers or {}
        assert headers.get("x-retry-exhausted") is True, (
            f"Terminal async DLX message should be marked exhausted; headers={headers!r}"
        )
        assert "x-processing-error" in headers, (
            f"Async DLX message should carry the processing error reason; headers={headers!r}"
        )
        assert headers.get("x-cycle-count") == 1, (
            f"Cycle count should increment to 1 on first failure; headers={headers!r}"
        )
    finally:
        await runner.stop()
        await consumer.close()
