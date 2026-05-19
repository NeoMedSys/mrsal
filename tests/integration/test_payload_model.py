"""Pydantic ``payload_model`` round-trip against a real broker.

Closes Issue 1c regression: when ``payload_model`` is set, the callback must
receive the *validated* model instance — not the raw bytes body. Pre-fix,
``validate_payload`` constructed the model and dropped the result, so users
still had to ``MyModel(**json.loads(body))`` inside every callback.
"""
import json
import threading

import pytest

from mrsal.amqp.subclass import MrsalBlockingAMQP

from tests.conftest import ExpectedPayload
from tests.integration.conftest import SyncConsumerRunner, broker_setup_args


@pytest.mark.integration
def test_callback_receives_validated_model_instance(unique_suffix, cleanup_topology):
    exchange = f"mrsal.it.pm.{unique_suffix}"
    queue = f"mrsal.it.pm.{unique_suffix}.q"
    routing_key = f"mrsal.it.pm.{unique_suffix}.rk"
    cleanup_topology.exchange(exchange)
    cleanup_topology.queue(queue)

    received: list = []
    callback_done = threading.Event()

    consumer = MrsalBlockingAMQP(**broker_setup_args())
    runner = SyncConsumerRunner(consumer)

    def on_message(method_frame, properties, body):
        # ``body`` must be the validated ExpectedPayload instance, not bytes.
        received.append(body)
        callback_done.set()

    runner.start(
        queue_name=queue,
        exchange_name=exchange,
        exchange_type="direct",
        routing_key=routing_key,
        callback=on_message,
        payload_model=ExpectedPayload,
        auto_ack=False,
        dlx_enable=False,
        enable_retry_cycles=False,
        use_quorum_queues=False,
        inactivity_timeout=1,
    )

    payload_obj = {"id": 7, "name": "samurai", "active": True}
    publisher = MrsalBlockingAMQP(**broker_setup_args())
    try:
        runner.wait_ready()

        publisher.publish_message(
            exchange_name=exchange,
            routing_key=routing_key,
            message=json.dumps(payload_obj).encode("utf-8"),
            exchange_type="direct",
            queue_name=queue,
            auto_declare=False,
        )

        assert callback_done.wait(timeout=10), "Callback was never invoked"
        assert len(received) == 1
        delivered = received[0]
        assert isinstance(delivered, ExpectedPayload), (
            f"Callback got {type(delivered).__name__} instead of ExpectedPayload — "
            "the validated instance was dropped (Issue 1c regression)"
        )
        assert delivered.id == 7
        assert delivered.name == "samurai"
        assert delivered.active is True
    finally:
        runner.stop()
        publisher.close()
        consumer.close()

    runner.raise_if_thread_errored()
