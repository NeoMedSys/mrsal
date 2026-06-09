"""In-memory test broker for mrsal.

Unit-test handlers built on ``MrsalBlockingAMQP`` / ``MrsalAsyncAMQP`` without a
live RabbitMQ. Routing, DLX topology and the consumer's real validate ->
callback -> ack/DLX logic run against an in-process broker, so tests exercise
mrsal's actual code paths instead of mocks.

	from mrsal.amqp.subclass import MrsalBlockingAMQP
	from mrsal.testing import TestMrsalBroker

	consumer = MrsalBlockingAMQP(host="x", port=5672, credentials=("g", "g"), virtual_host="/")
	with TestMrsalBroker(consumer) as br:
		br.register_consumer(
			queue_name="orders", exchange_name="orders.x",
			exchange_type="direct", routing_key="orders.new",
			callback=handle_order, payload_model=OrderEvent,
		)
		br.publish({"order_id": "abc", "amount": 10.0},
					exchange="orders.x", routing_key="orders.new")
		# handler ran inline; assert side effects
"""
from mrsal.testing._broker import InMemoryBroker, StoredMessage, topic_matches
from mrsal.testing._context import TestMrsalBroker, TestMrsalAsyncBroker

__all__ = [
	"InMemoryBroker",
	"StoredMessage",
	"TestMrsalBroker",
	"TestMrsalAsyncBroker",
	"topic_matches",
]
