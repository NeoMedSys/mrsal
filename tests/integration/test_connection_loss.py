"""Broker-side connection loss against a real broker (#105).

The broker force-closes client connections through the management HTTP API,
the way a broker restart or a network cut looks to the client. Covers the two
production failures from #105:

  * the blocking publisher hitting a dead socket inside the passive declare
    must reconnect and publish, not raise ``MrsalAbortedSetup``;
  * the async consumer must keep consuming after its connection is closed
    under it, not sit on an iterator that never yields again.

Each test runs in its own throwaway vhost and force-closes only that vhost's
connections, so it cannot disturb other tests or other clients on a shared
broker. (Matching by client port does not work: Docker's port proxy rewrites
the client address the broker sees.) The test user must be allowed to create
vhosts, which ``guest`` is on the docker-compose broker.

Management API endpoint defaults to ``localhost:15673`` (the port mapped in
``docker-compose.yml``); override with ``MRSAL_IT_MGMT_PORT``.
"""
import asyncio
import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager

import pika
import pytest

from mrsal import config
from mrsal.amqp.subclass import MrsalAsyncAMQP, MrsalBlockingPublisher

from tests.integration.conftest import (
    AsyncConsumerRunner,
    BROKER_HOST,
    BROKER_PASS,
    BROKER_PORT,
    BROKER_USER,
    broker_setup_args,
)


MGMT_PORT = int(os.environ.get("MRSAL_IT_MGMT_PORT", "15673"))
# Longer than aio-pika's default robust reconnect interval (5s) plus the
# management API's stats interval, so a stray reconnect would be visible.
RECONNECT_GRACE_SEC = 12


def _mgmt_request(method: str, path: str, body: dict | None = None):
    auth = base64.b64encode(f"{BROKER_USER}:{BROKER_PASS}".encode()).decode()
    request = urllib.request.Request(
        url=f"http://{BROKER_HOST}:{MGMT_PORT}/api/{path}",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
            "X-Reason": "mrsal #105 integration test",
        },
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = response.read()
    return json.loads(payload) if payload else None


@pytest.fixture
def vhost(unique_suffix):
    """A throwaway vhost for one test; deleting it removes all its topology."""
    name = f"mrsal-it-cl-{unique_suffix}"
    quoted = urllib.parse.quote(name, safe="")
    _mgmt_request(method="PUT", path=f"vhosts/{quoted}")
    try:
        _mgmt_request(
            method="PUT",
            path=f"permissions/{quoted}/{urllib.parse.quote(BROKER_USER, safe='')}",
            body={"configure": ".*", "write": ".*", "read": ".*"},
        )
        yield name
    finally:
        _mgmt_request(method="DELETE", path=f"vhosts/{quoted}")


def force_close_vhost_connections(vhost: str, timeout: float = 15.0) -> int:
    """Close every client connection in ``vhost`` from the broker side; return how many.

    Polls because the management API lists a fresh connection only once the
    broker has emitted its stats.
    """
    path = f"vhosts/{urllib.parse.quote(vhost, safe='')}/connections"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        closed = 0
        for connection in _mgmt_request(method="GET", path=path):
            name = urllib.parse.quote(connection["name"], safe="")
            try:
                _mgmt_request(method="DELETE", path=f"connections/{name}")
            except urllib.error.HTTPError as e:
                # Listed but already closed: one of the test's own short-lived
                # setup connections, gone before the listing caught up.
                if e.code != 404:
                    raise
            else:
                closed += 1
        if closed:
            return closed
        time.sleep(0.25)
    raise AssertionError(f"No open connections listed in vhost {vhost} within {timeout}s")


@contextmanager
def _channel(vhost: str):
    """A short-lived pika channel in ``vhost`` for setup and inspection."""
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=BROKER_HOST,
            port=BROKER_PORT,
            credentials=pika.PlainCredentials(BROKER_USER, BROKER_PASS),
            virtual_host=vhost,
            heartbeat=30,
        )
    )
    try:
        yield conn.channel()
    finally:
        conn.close()


def _declare_target(vhost: str, exchange: str, queue: str, routing_key: str) -> None:
    with _channel(vhost=vhost) as ch:
        ch.exchange_declare(exchange=exchange, exchange_type="direct", durable=True)
        ch.queue_declare(queue=queue, durable=True)
        ch.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)


def _get_body(vhost: str, queue: str) -> bytes | None:
    with _channel(vhost=vhost) as ch:
        _method, _properties, body = ch.basic_get(queue=queue, auto_ack=True)
    return body


@pytest.mark.integration
def test_publisher_recovers_when_connection_dies_before_passive_declare(vhost):
    # Two targets: the first publish warms the connection, the second forces a
    # fresh passive declare on the socket the broker has since closed.
    first = ("cl.a", "cl.a.q", "rk-a")
    second = ("cl.b", "cl.b.q", "rk-b")
    for exchange, queue, routing_key in (first, second):
        _declare_target(vhost=vhost, exchange=exchange, queue=queue, routing_key=routing_key)

    with MrsalBlockingPublisher(**broker_setup_args(virtual_host=vhost)) as publisher:
        publisher.publish(
            exchange_name=first[0], queue_name=first[1], routing_key=first[2],
            exchange_type="direct", message=b"before",
        )
        assert force_close_vhost_connections(vhost=vhost) >= 1

        # pika has not read the Connection.Close yet, so the publisher still
        # believes its connection is open and runs the passive declare on it.
        publisher.publish(
            exchange_name=second[0], queue_name=second[1], routing_key=second[2],
            exchange_type="direct", message=b"after",
        )

    assert _get_body(vhost=vhost, queue=first[1]) == b"before"
    assert _get_body(vhost=vhost, queue=second[1]) == b"after"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_consumer_keeps_consuming_after_connection_is_closed(vhost):
    exchange, queue, routing_key = "cl.async", "cl.async.q", "cl.async.rk"

    received: list[bytes] = []
    got_message = asyncio.Event()

    async def on_message(message, properties, body):
        received.append(body)
        got_message.set()

    consumer = MrsalAsyncAMQP(**broker_setup_args(virtual_host=vhost))
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
        with _channel(vhost=vhost) as ch:
            ch.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

    try:
        await runner.wait_ready()
        first_connection = consumer._connection
        await asyncio.to_thread(publish, b"before")
        await asyncio.wait_for(got_message.wait(), timeout=10)
        got_message.clear()

        assert await asyncio.to_thread(force_close_vhost_connections, vhost) >= 1

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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rebuild_after_timed_out_closes_leaves_one_consumer(vhost, monkeypatch):
    """A close that times out drops the handle while it may still be open. The
    dropped robust connection must not reconnect and restore its consumer next
    to the rebuilt one."""
    monkeypatch.setattr(config, "CLOSE_TIMEOUT_SEC", 1e-9)  # every close times out
    exchange, queue, routing_key = "cl.zombie", "cl.zombie.q", "cl.zombie.rk"

    async def on_message(message, properties, body):
        pass

    consumer = MrsalAsyncAMQP(**broker_setup_args(virtual_host=vhost))
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

    try:
        await runner.wait_ready()
        first_connection = consumer._connection
        assert await asyncio.to_thread(force_close_vhost_connections, vhost) >= 1

        deadline = time.monotonic() + 30
        while consumer._connection in (None, first_connection):
            assert time.monotonic() < deadline, "consumer was not rebuilt"
            await asyncio.sleep(0.25)
        await asyncio.sleep(RECONNECT_GRACE_SEC)

        quoted = urllib.parse.quote(vhost, safe="")
        queue_info = await asyncio.to_thread(_mgmt_request, method="GET", path=f"queues/{quoted}/{queue}")
        connections = await asyncio.to_thread(_mgmt_request, method="GET", path=f"vhosts/{quoted}/connections")
        assert queue_info["consumers"] == 1
        assert len(connections) == 1
    finally:
        await runner.stop()
