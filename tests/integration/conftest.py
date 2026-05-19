"""Shared fixtures and helpers for integration tests.

The broker is expected to be reachable on the address resolved from
``MRSAL_IT_HOST`` / ``MRSAL_IT_PORT`` (default ``localhost:5673``) with
``MRSAL_IT_USER`` / ``MRSAL_IT_PASS`` (default ``guest`` / ``guest``).

If the broker is not reachable, the whole module is skipped with a clear
message instead of producing a wall of connection-refused tracebacks.
"""
import asyncio
import os
import threading
import time
import uuid
from contextlib import contextmanager

import pika
import pytest


BROKER_HOST = os.environ.get("MRSAL_IT_HOST", "localhost")


def _resolve_broker_port() -> int:
    """Parse ``MRSAL_IT_PORT`` with a clear error if it isn't an integer.

    Module-load parse failures otherwise surface as a confusing ``ValueError``
    during collection — a misconfigured env var should fail loudly with the
    actual offending value.
    """
    raw = os.environ.get("MRSAL_IT_PORT", "5673")
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"MRSAL_IT_PORT must be an integer, got {raw!r}"
        ) from exc


BROKER_PORT = _resolve_broker_port()
BROKER_USER = os.environ.get("MRSAL_IT_USER", "guest")
BROKER_PASS = os.environ.get("MRSAL_IT_PASS", "guest")
BROKER_VHOST = os.environ.get("MRSAL_IT_VHOST", "/")


def _broker_reachable() -> bool:
    """Open a short AMQP connection so wrong-creds / wrong-vhost also skip.

    A pure TCP probe wrongly reports "broker up" when the port is open but
    auth is misconfigured, leaving the tests to fail with
    ``ProbableAuthenticationError`` everywhere. The brief AMQP handshake
    costs ~0.5s once per session but produces a useful skip reason.
    """
    try:
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=BROKER_HOST,
                port=BROKER_PORT,
                credentials=pika.PlainCredentials(BROKER_USER, BROKER_PASS),
                virtual_host=BROKER_VHOST,
                heartbeat=10,
                blocked_connection_timeout=5,
                socket_timeout=3,
            )
        )
        conn.close()
        return True
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    """Skip every integration test if the broker isn't usable from this run."""
    if _broker_reachable():
        return
    skip = pytest.mark.skip(
        reason=(
            f"RabbitMQ broker not reachable at {BROKER_HOST}:{BROKER_PORT} "
            f"as {BROKER_USER}@{BROKER_VHOST} — start it with "
            "`docker compose -f tests/integration/docker-compose.yml up -d`"
        )
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


def broker_setup_args(**overrides) -> dict:
    """Kwargs for instantiating ``MrsalBlockingAMQP`` / ``MrsalAsyncAMQP``."""
    args = {
        "host": BROKER_HOST,
        "port": BROKER_PORT,
        "credentials": (BROKER_USER, BROKER_PASS),
        "virtual_host": BROKER_VHOST,
        "heartbeat": 60,
        "prefetch_count": 5,
    }
    args.update(overrides)
    return args


@pytest.fixture
def unique_suffix() -> str:
    """Short unique tag so concurrent test runs don't collide on queue names."""
    return uuid.uuid4().hex[:8]


@contextmanager
def raw_pika_channel():
    """Open a short-lived pika channel for setup / teardown / inspection.

    Kept separate from the mrsal-owned connection so that broker state can be
    inspected even when the mrsal consumer/publisher channels are closed mid-test.
    """
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=BROKER_HOST,
            port=BROKER_PORT,
            credentials=pika.PlainCredentials(BROKER_USER, BROKER_PASS),
            virtual_host=BROKER_VHOST,
            heartbeat=30,
        )
    )
    try:
        yield conn.channel()
    finally:
        try:
            conn.close()
        except Exception:
            pass


@pytest.fixture
def cleanup_topology():
    """Track exchanges/queues to delete after the test.

    Append names with ``cleanup_topology.exchange(name)`` / ``.queue(name)``;
    teardown runs even if the test raises. Deletion failures are swallowed so
    a missing resource (e.g. never declared) doesn't mask the real test error.
    """
    exchanges: list[str] = []
    queues: list[str] = []

    class _Tracker:
        def exchange(self, name: str) -> None:
            exchanges.append(name)

        def queue(self, name: str) -> None:
            queues.append(name)

    tracker = _Tracker()
    yield tracker

    with raw_pika_channel() as ch:
        for q in queues:
            try:
                ch.queue_delete(queue=q)
            except Exception:
                pass
        for e in exchanges:
            try:
                ch.exchange_delete(exchange=e)
            except Exception:
                pass


class SyncConsumerRunner:
    """Run ``MrsalBlockingAMQP.start_consumer`` in a daemon thread.

    Wraps the boilerplate every sync integration test was repeating:
    thread, error capture, wait-until-declared, and clean iterator cancel.
    ``wait_ready`` surfaces a consumer-thread exception immediately instead
    of letting the test fail later with a stale "never finished declaring"
    message.
    """

    def __init__(self, consumer):
        self.consumer = consumer
        self.errors: list[Exception] = []
        self._thread: threading.Thread | None = None

    def start(self, **start_consumer_kwargs) -> None:
        def _run():
            try:
                self.consumer.start_consumer(**start_consumer_kwargs)
            except Exception as exc:
                self.errors.append(exc)

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def wait_ready(self, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.errors:
                raise AssertionError(
                    f"Consumer thread raised before topology declared: {self.errors[0]!r}"
                )
            if (
                self.consumer._consumer_channel is not None
                and self.consumer.auto_declare_ok
            ):
                return
            time.sleep(0.05)
        raise AssertionError(
            f"Consumer didn't finish declaring topology within {timeout}s"
        )

    def stop(self, timeout: float = 10.0) -> None:
        c = self.consumer
        if c._consumer_channel is not None and c._connection is not None:
            try:
                c._connection.add_callback_threadsafe(c._consumer_channel.cancel)
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def raise_if_thread_errored(self) -> None:
        if self.errors:
            raise AssertionError(f"Consumer thread raised: {self.errors[0]!r}")


class AsyncConsumerRunner:
    """Run ``MrsalAsyncAMQP.start_consumer`` in an asyncio task.

    Same role as ``SyncConsumerRunner`` for the async path. ``wait_ready``
    fails fast if the consumer task has already errored, so the surrounding
    test gets the real exception instead of a "never declared" timeout.
    """

    def __init__(self, consumer):
        self.consumer = consumer
        self._task: asyncio.Task | None = None

    def start(self, **start_consumer_kwargs) -> None:
        self._task = asyncio.create_task(
            self.consumer.start_consumer(**start_consumer_kwargs)
        )

    async def wait_ready(self, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._task is not None and self._task.done():
                exc = self._task.exception()
                if exc is not None:
                    raise AssertionError(
                        f"Async consumer task errored before ready: {exc!r}"
                    ) from exc
            if self.consumer.auto_declare_ok and self.consumer._channel is not None:
                return
            await asyncio.sleep(0.05)
        raise AssertionError(
            f"Async consumer didn't finish declaring topology within {timeout}s"
        )

    async def stop(self, timeout: float = 10.0) -> None:
        await self.consumer.stop()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=timeout)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except (asyncio.CancelledError, Exception):
                    pass
