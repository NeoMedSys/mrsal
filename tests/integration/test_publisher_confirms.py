"""Publisher confirms wired up on the user-facing publish path.

Closes the gap behind Issue 1a: ``publish_message`` is decorated to retry on
``NackError`` / ``UnroutableError``, but without ``confirm_delivery()`` on the
publish channel those exceptions can never fire. This test sets up a queue
that rejects publishes once it's full and asserts the broker NACK actually
surfaces (instead of returning silently and producing data loss).

The queue is declared with raw pika because mrsal's auto-declare in
``publish_message`` doesn't accept queue-overflow parameters. We then call
``publish_message(auto_declare=False)`` so it publishes against the topology
we've prepared.
"""
import pika
import pytest
from pika.exceptions import NackError
from tenacity import RetryError

from mrsal.amqp.subclass import MrsalBlockingAMQP

from tests.integration.conftest import broker_setup_args, raw_pika_channel


@pytest.mark.integration
def test_publish_to_full_reject_publish_queue_surfaces_nack(
    unique_suffix, cleanup_topology,
):
    exchange = f"mrsal.it.conf.{unique_suffix}"
    queue = f"mrsal.it.conf.{unique_suffix}.q"
    routing_key = f"mrsal.it.conf.{unique_suffix}.rk"
    cleanup_topology.exchange(exchange)
    cleanup_topology.queue(queue)

    # x-max-length=1 + x-overflow=reject-publish gives us a deterministic
    # NACK from the broker on the second publish. Classic queue (not quorum)
    # because reject-publish semantics are simplest to reason about there.
    with raw_pika_channel() as ch:
        ch.exchange_declare(
            exchange=exchange, exchange_type="direct", durable=True,
        )
        ch.queue_declare(
            queue=queue,
            durable=True,
            arguments={
                "x-max-length": 1,
                "x-overflow": "reject-publish",
            },
        )
        ch.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key)

    publisher = MrsalBlockingAMQP(**broker_setup_args())

    def publish_once(body: bytes) -> None:
        publisher.publish_message(
            exchange_name=exchange,
            routing_key=routing_key,
            message=body,
            exchange_type="direct",
            queue_name=queue,
            auto_declare=False,
            prop=pika.BasicProperties(delivery_mode=2),
        )

    try:
        publish_once(b"first-fits")

        # The second publish targets a full reject-publish queue. With
        # ``confirm_delivery()`` wired the broker NACK has to surface as
        # ``NackError`` (and tenacity retries it twice more before giving up
        # with ``RetryError`` wrapping the underlying NACK). Without confirms
        # the publish would return successfully and this test would hang here
        # — that's the regression we're guarding against.
        with pytest.raises((NackError, RetryError)) as exc_info:
            publish_once(b"second-rejected")

        raised = exc_info.value
        if isinstance(raised, RetryError):
            inner = raised.last_attempt.exception()
            assert isinstance(inner, NackError), (
                f"Expected NackError under RetryError, got {type(inner).__name__}: {inner!r}"
            )
    finally:
        publisher.close()
