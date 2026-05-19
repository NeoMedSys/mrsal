"""Async consumer concurrency exercised against a real broker.

Follow-up to #74: ``max_concurrent_tasks`` was added so the async consumer can
process N messages in parallel, bounded by an ``asyncio.Semaphore``. Mocked
unit tests can verify the dispatch logic but not that the broker keeps feeding
deliveries under load. This test checks that, with prefetch and concurrent
tasks both > 1, the consumer actually observes multiple in-flight callbacks
at the same time.
"""
import asyncio

import pika
import pytest

from mrsal.amqp.subclass import MrsalAsyncAMQP

from tests.integration.conftest import AsyncConsumerRunner, broker_setup_args, raw_pika_channel


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_consumer_processes_messages_concurrently(
    unique_suffix, cleanup_topology,
):
    exchange = f"mrsal.it.cc.{unique_suffix}"
    queue = f"mrsal.it.cc.{unique_suffix}.q"
    routing_key = f"mrsal.it.cc.{unique_suffix}.rk"
    cleanup_topology.exchange(exchange)
    cleanup_topology.queue(queue)

    max_concurrent_tasks = 4
    num_messages = 8

    # asyncio is single-threaded, so these counters don't need locking — every
    # ``+= 1`` runs to completion between awaits.
    state = {"active": 0, "max_observed": 0, "completed": 0}
    done = asyncio.Event()

    async def callback(message, properties, body):
        state["active"] += 1
        state["max_observed"] = max(state["max_observed"], state["active"])
        try:
            # Hold the slot long enough that the next iterator pull happens
            # while this task is still active — that's what surfaces concurrency.
            await asyncio.sleep(0.3)
        finally:
            state["active"] -= 1
            state["completed"] += 1
            if state["completed"] >= num_messages:
                done.set()

    consumer = MrsalAsyncAMQP(**broker_setup_args(prefetch_count=num_messages))
    runner = AsyncConsumerRunner(consumer)
    runner.start(
        queue_name=queue,
        exchange_name=exchange,
        exchange_type="direct",
        routing_key=routing_key,
        callback=callback,
        auto_ack=False,
        dlx_enable=False,
        enable_retry_cycles=False,
        use_quorum_queues=False,
        max_concurrent_tasks=max_concurrent_tasks,
    )

    try:
        await runner.wait_ready()

        # Publish in a thread to avoid blocking the loop while pika does I/O.
        def publish_all() -> None:
            with raw_pika_channel() as ch:
                for i in range(num_messages):
                    ch.basic_publish(
                        exchange=exchange,
                        routing_key=routing_key,
                        body=f"msg-{i}".encode(),
                        properties=pika.BasicProperties(delivery_mode=2),
                    )

        await asyncio.to_thread(publish_all)

        await asyncio.wait_for(done.wait(), timeout=20)
    finally:
        await runner.stop()
        await consumer.close()

    assert state["completed"] == num_messages, (
        f"Expected {num_messages} messages processed, got {state['completed']}"
    )
    # With prefetch_count >= max_concurrent_tasks and async-sleeping callbacks,
    # the semaphore bound should be reached. Anything below ``max-1`` means
    # the dispatch path regressed to near-sequential — the whole point of #74.
    # The ``-1`` slack absorbs scheduling jitter on heavily loaded CI machines
    # without weakening the regression signal.
    assert state["max_observed"] >= max_concurrent_tasks - 1, (
        f"Expected near-saturated concurrency (>= {max_concurrent_tasks - 1}); "
        f"max observed in-flight was {state['max_observed']}"
    )
    assert state["max_observed"] <= max_concurrent_tasks, (
        f"Semaphore bound violated: max observed in-flight was "
        f"{state['max_observed']}, bound is {max_concurrent_tasks}"
    )
