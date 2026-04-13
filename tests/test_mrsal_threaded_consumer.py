import pytest
from unittest.mock import MagicMock, patch, ANY
from mrsal.amqp.subclass import MrsalBlockingAMQP
from pydantic.dataclasses import dataclass

@dataclass
class ExpectedPayload:
	id: int
	name: str
	active: bool

# --- Fixtures ---

@pytest.fixture
def mock_consumer():
	"""Create a mock consumer with mocked connection and channel."""
	consumer = MrsalBlockingAMQP(
		host="localhost",
		port=5672,
		credentials=("user", "password"),
		virtual_host="testboi",
		ssl=False,
		verbose=True,
		prefetch_count=1,
		heartbeat=60
	)

	# Mock critical Pika objects
	consumer._connection = MagicMock()
	consumer._channel = MagicMock()
	consumer._consumer_channel = consumer._channel
	consumer._connection.channel.return_value = consumer._channel
	consumer.auto_declare_ok = True

	# Mock setup methods to prevent actual network calls
	consumer.setup_blocking_connection = MagicMock()
	consumer._setup_exchange_and_queue = MagicMock()

	return consumer

# --- Tests ---

def test_threaded_flag_uses_thread_pool(mock_consumer):
	"""Test that threaded=True uses a ThreadPoolExecutor."""

	# 1. Setup a generator to simulate one message then stop
	mock_method = MagicMock()
	mock_method.delivery_tag = 1
	mock_props = MagicMock()
	mock_props.headers = {}
	mock_body = b'{"id": 1, "name": "ThreadTest", "active": true}'

	# Simulate consume yielding one message
	mock_consumer._channel.consume.return_value = [(mock_method, mock_props, mock_body)]

	# 2. Mock ThreadPoolExecutor to verify it gets used
	with patch('mrsal.amqp.subclass.ThreadPoolExecutor') as mock_pool_cls:
		mock_pool_instance = MagicMock()
		mock_pool_cls.return_value = mock_pool_instance

		# 3. Run start_consumer with threaded=True
		mock_consumer.start_consumer(
			queue_name="test_queue",
			callback=MagicMock(),
			threaded=True,
			auto_ack=False,
			auto_declare=False,
			payload_model=None
		)

		# 4. Verify a pool was created and submit was called
		mock_pool_cls.assert_called_once_with(max_workers=mock_consumer.prefetch_count)
		mock_pool_instance.submit.assert_called_once_with(
			mock_consumer._process_single_message,
			mock_method, mock_props, mock_body, ANY
		)
		mock_pool_instance.shutdown.assert_called_once_with(wait=True, cancel_futures=True)

def test_blocking_mode_does_not_use_thread_pool(mock_consumer):
	"""Test that threaded=False (default) does not create a ThreadPoolExecutor."""

	mock_method = MagicMock()
	mock_props = MagicMock()
	mock_body = b'test'
	mock_consumer._channel.consume.return_value = [(mock_method, mock_props, mock_body)]

	with patch('mrsal.amqp.subclass.ThreadPoolExecutor') as mock_pool_cls:
		# Run default (threaded=False)
		mock_consumer.start_consumer(
			queue_name="test_queue",
			callback=MagicMock(),
			threaded=False,
			auto_ack=False,
			auto_declare=False
		)

		# Assert NO pool was created
		mock_pool_cls.assert_not_called()

def test_schedule_threadsafe_behavior(mock_consumer):
	"""Test that _schedule_threadsafe chooses the right Pika method."""

	mock_func = MagicMock()
	arg1 = "test"

	# Case A: Threaded = True -> Use add_callback_threadsafe
	mock_consumer._schedule_threadsafe(mock_func, True, arg1)
	mock_consumer._connection.add_callback_threadsafe.assert_called_once()
	# The function itself shouldn't be called immediately, but wrapped
	mock_func.assert_not_called()

	# Reset
	mock_consumer._connection.reset_mock()

	# Case B: Threaded = False -> Call immediately
	mock_consumer._schedule_threadsafe(mock_func, False, arg1)
	mock_consumer._connection.add_callback_threadsafe.assert_not_called()
	mock_func.assert_called_once_with(arg1)

def test_worker_logic_acks_threadsafe(mock_consumer):
	"""
	Test that the worker logic (_process_single_message) correctly
	uses the threadsafe scheduling for ACKs when threaded=True.
	"""
	mock_method = MagicMock()
	mock_method.delivery_tag = 99
	mock_props = MagicMock()
	mock_props.headers = {}
	mock_body = b'{}'

	# Config passed to worker
	runtime_config = {
		'auto_ack': False,
		'threaded': True,
		'callback': MagicMock(),
		'dlx_enable': False,
		'enable_retry_cycles': False
	}

	# Spy on _schedule_threadsafe to ensure it's called
	with patch.object(mock_consumer, '_schedule_threadsafe') as mock_schedule:
		mock_consumer._process_single_message(mock_method, mock_props, mock_body, runtime_config)

		# Verify ACK was scheduled safely
		mock_schedule.assert_called_with(
			mock_consumer._consumer_channel.basic_ack,
			True,
			delivery_tag=99
		)

def test_worker_logic_acks_blocking(mock_consumer):
	"""Test that worker logic calls ACK directly when threaded=False."""
	mock_method = MagicMock()
	mock_method.delivery_tag = 99
	mock_props = MagicMock()
	mock_props.headers = {}

	runtime_config = {
		'auto_ack': False,
		'threaded': False,
		'callback': MagicMock(),
		'dlx_enable': False
	}

	with patch.object(mock_consumer, '_schedule_threadsafe') as mock_schedule:
		mock_consumer._process_single_message(mock_method, mock_props, b'{}', runtime_config)

		# Verify schedule was called with threaded=False
		mock_schedule.assert_called_with(
			mock_consumer._consumer_channel.basic_ack,
			False,
			delivery_tag=99
		)

def test_max_workers_defaults_to_prefetch_count(mock_consumer):
	"""Test that max_workers defaults to prefetch_count when not specified."""
	mock_consumer._channel.consume.return_value = []

	with patch('mrsal.amqp.subclass.ThreadPoolExecutor') as mock_pool_cls:
		mock_pool_instance = MagicMock()
		mock_pool_cls.return_value = mock_pool_instance

		mock_consumer.start_consumer(
			queue_name="test_queue",
			callback=MagicMock(),
			threaded=True,
			auto_ack=False,
			auto_declare=False
		)

		mock_pool_cls.assert_called_once_with(max_workers=mock_consumer.prefetch_count)

def test_max_workers_custom_value(mock_consumer):
	"""Test that an explicit max_workers value is respected."""
	mock_consumer._channel.consume.return_value = []

	with patch('mrsal.amqp.subclass.ThreadPoolExecutor') as mock_pool_cls:
		mock_pool_instance = MagicMock()
		mock_pool_cls.return_value = mock_pool_instance

		mock_consumer.start_consumer(
			queue_name="test_queue",
			callback=MagicMock(),
			threaded=True,
			max_workers=10,
			auto_ack=False,
			auto_declare=False
		)

		mock_pool_cls.assert_called_once_with(max_workers=10)
