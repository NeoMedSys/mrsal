"""End-to-end publish/consume against a real broker.

This is the smallest meaningful integration test: it exercises the user-facing
``publish_message`` and ``start_consumer`` paths and asserts the consumer
callback observes the published body. Catches whole classes of regressions the
mock-based unit tests can't (channel/exchange/queue declare ordering, prefetch,
publisher confirms wiring, basic_ack delivery_tag flow).
"""
import threading

import pytest

from mrsal.amqp.subclass import MrsalBlockingAMQP

from tests.integration.conftest import SyncConsumerRunner, broker_setup_args


@pytest.mark.integration
def test_publish_then_consume_delivers_body(unique_suffix, cleanup_topology):
    exchange = f"mrsal.it.rt.{unique_suffix}"
    queue = f"mrsal.it.rt.{unique_suffix}.q"
    routing_key = f"mrsal.it.rt.{unique_suffix}.rk"
    cleanup_topology.exchange(exchange)
    cleanup_topology.queue(queue)

    payload = b"round-trip-body"
    received: list[bytes] = []
    callback_done = threading.Event()

    consumer = MrsalBlockingAMQP(**broker_setup_args())
    runner = SyncConsumerRunner(consumer)

    def on_message(method_frame, properties, body):
        received.append(body)
        callback_done.set()

    # use_quorum_queues=True exercises the broker actually accepting the
    # ``x-queue-type=quorum`` declare. Mock tests can't validate broker-side
    # acceptance of those args, so doing it here gives us a regression guard
    # on the library's default queue type for free.
    runner.start(
        queue_name=queue,
        exchange_name=exchange,
        exchange_type="direct",
        routing_key=routing_key,
        callback=on_message,
        auto_ack=False,
        dlx_enable=False,
        enable_retry_cycles=False,
        use_quorum_queues=True,
        inactivity_timeout=1,
    )

    publisher = MrsalBlockingAMQP(**broker_setup_args())
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

        assert callback_done.wait(timeout=10), "Callback was never invoked"
        assert received == [payload]
    finally:
        runner.stop()
        publisher.close()
        consumer.close()

    runner.raise_if_thread_errored()
