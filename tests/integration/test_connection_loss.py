"""Broker-side connection loss against a real broker (#105).

The broker force-closes every client connection through the management HTTP
API, the way a broker restart or a network cut looks to the client. Covers
the two production failures from #105:

  * the blocking publisher hitting a dead socket inside the passive declare
    must reconnect and publish, not raise ``MrsalAbortedSetup``;
  * the async consumer must keep consuming after its connection is closed
    under it, not sit on an iterator that never yields again.

Management API endpoint defaults to ``localhost:15673`` (the port mapped in
``docker-compose.yml``); override with ``MRSAL_IT_MGMT_PORT``.
"""
import asyncio
import base64
import json
import os
import time
import urllib.parse
import urllib.request

import pytest

from mrsal.amqp.subclass import MrsalAsyncAMQP, MrsalBlockingPublisher

from tests.integration.conftest import (
    AsyncConsumerRunner,
    BROKER_HOST,
    BROKER_PASS,
    BROKER_USER,
    broker_setup_args,
    raw_pika_channel,
)


MGMT_PORT = int(os.environ.get("MRSAL_IT_MGMT_PORT", "15673"))


def _mgmt_request(method: str, path: str):
    auth = base64.b64encode(f"{BROKER_USER}:{BROKER_PASS}".encode()).decode()
    request = urllib.request.Request(
        url=f"http://{BROKER_HOST}:{MGMT_PORT}/api/{path}",
        method=method,
        headers={"Authorization": f"Basic {auth}", "X-Reason": "mrsal #105 integration test"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        body = response.read()
    return json.loads(body) if body else None


def force_close_all_connections(timeout: float = 15.0) -> int:
    """Close every client connection from the broker side; return how many.

    Polls because the management API lists a fresh connection only once the
    broker has emitted its stats.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        connections = _mgmt_request(method="GET", path="connections")
        if connections:
            for connection in connections:
                name = urllib.parse.quote(connection["name"], safe="")
                _mgmt_request(method="DELETE", path=f"connections/{name}")
            return len(connections)
        time.sleep(0.25)
    raise AssertionError(f"No connections listed by the management API within {timeout}s")


def _declare_target(exchange: str, queue: str, routing_key: str) -> None:
    with raw_pika_channel() as ch:
        ch.exchange_declare(exchange=exchange, exchange_type="direct", durable=True)
        ch.queue_declare(queue=queue, durable=True)
        ch.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)


def _get_body(queue: str) -> bytes | None:
    with raw_pika_channel() as ch:
        _method, _properties, body = ch.basic_get(queue=queue, auto_ack=True)
    return body


@pytest.mark.integration
def test_publisher_recovers_when_connection_dies_before_passive_declare(unique_suffix, cleanup_topology):
    # Two targets: the first publish warms the connection, the second forces a
    # fresh passive declare on the socket the broker has since closed.
    first = (f"mrsal.it.cl.{unique_suffix}.a", f"mrsal.it.cl.{unique_suffix}.a.q", "rk-a")
    second = (f"mrsal.it.cl.{unique_suffix}.b", f"mrsal.it.cl.{unique_suffix}.b.q", "rk-b")
    for exchange, queue, routing_key in (first, second):
        cleanup_topology.exchange(exchange)
        cleanup_topology.queue(queue)
        _declare_target(exchange=exchange, queue=queue, routing_key=routing_key)

    with MrsalBlockingPublisher(**broker_setup_args()) as publisher:
        publisher.publish(
            exchange_name=first[0], queue_name=first[1], routing_key=first[2],
            exchange_type="direct", message=b"before",
        )
        assert force_close_all_connections() >= 1

        # pika has not read the Connection.Close yet, so the publisher still
        # believes its connection is open and runs the passive declare on it.
        publisher.publish(
            exchange_name=second[0], queue_name=second[1], routing_key=second[2],
            exchange_type="direct", message=b"after",
        )

    assert _get_body(queue=first[1]) == b"before"
    assert _get_body(queue=second[1]) == b"after"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_consumer_keeps_consuming_after_connection_is_closed(unique_suffix, cleanup_topology):
    exchange = f"mrsal.it.cl.{unique_suffix}.async"
    queue = f"mrsal.it.cl.{unique_suffix}.async.q"
    routing_key = f"mrsal.it.cl.{unique_suffix}.async.rk"
    cleanup_topology.exchange(exchange)
    cleanup_topology.queue(queue)

    received: list[bytes] = []
    got_message = asyncio.Event()

    async def on_message(message, properties, body):
        received.append(body)
        got_message.set()

    consumer = MrsalAsyncAMQP(**broker_setup_args())
    runner = AsyncConsumerRunner(consumer)
    runner.start(
        queue_name=queue,
        exchange_name=exchange,
        exchange_type="direct",
        routing_key=routing_key,
        callback=on_message,
        auto_ack=False,
        dlx_enable=False,
        enable_retry_cycles=False,
        use_quorum_queues=False,
    )

    def publish(body: bytes) -> None:
        with raw_pika_channel() as ch:
            ch.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

    try:
        await runner.wait_ready()
        first_connection = consumer._connection
        await asyncio.to_thread(publish, b"before")
        await asyncio.wait_for(got_message.wait(), timeout=10)
        got_message.clear()

        assert await asyncio.to_thread(force_close_all_connections) >= 1

        # The queue is durable, so a message published while the consumer is
        # rebuilding waits for it.
        await asyncio.to_thread(publish, b"after")
        await asyncio.wait_for(got_message.wait(), timeout=30)

        assert received == [b"before", b"after"]
        assert not runner._task.done(), "consumer task must still be running"
        # Rebuilt by mrsal, not restored in place by aio-pika's robust reconnect.
        assert consumer._connection is not first_connection
    finally:
        await runner.stop()
