import pytest
import threading
import time
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
    consumer.auto_declare_ok = True
    
    # Mock setup methods to prevent actual network calls
    consumer.setup_blocking_connection = MagicMock()
    consumer._setup_exchange_and_queue = MagicMock()
    
    return consumer

# --- Tests ---

def test_threaded_flag_spawns_thread(mock_consumer):
    """Test that threaded=True actually starts a new thread."""
    
    # 1. Setup a generator to simulate one message then stop
    mock_method = MagicMock()
    mock_method.delivery_tag = 1
    mock_props = MagicMock()
    mock_props.headers = {}
    mock_body = b'{"id": 1, "name": "ThreadTest", "active": true}'
    
    # Simulate consume yielding one message
    mock_consumer._channel.consume.return_value = [(mock_method, mock_props, mock_body)]
    
    # 2. Mock threading.Thread to verify it gets called
    with patch('threading.Thread') as mock_thread_cls:
        mock_thread_instance = MagicMock()
        mock_thread_cls.return_value = mock_thread_instance
        
        # 3. Run start_consumer with threaded=True
        mock_consumer.start_consumer(
            queue_name="test_queue",
            callback=MagicMock(),
            threaded=True,  # <--- The Flag
            auto_ack=False,
            auto_declare=False, # Skip setup for unit test speed
            payload_model=None
        )
        
        # 4. Verify a thread was created and started
        assert mock_thread_cls.call_count == 1
        mock_thread_instance.start.assert_called_once()
        
        # Verify target is the internal worker method
        call_args = mock_thread_cls.call_args[1] # kwargs
        assert call_args['target'] == mock_consumer._process_single_message
        assert call_args['daemon'] is True

def test_blocking_mode_does_not_spawn_thread(mock_consumer):
    """Test that threaded=False (default) runs in the main thread."""
    
    mock_method = MagicMock()
    mock_props = MagicMock()
    mock_body = b'test'
    mock_consumer._channel.consume.return_value = [(mock_method, mock_props, mock_body)]
    
    with patch('threading.Thread') as mock_thread_cls:
        # Run default (threaded=False)
        mock_consumer.start_consumer(
            queue_name="test_queue",
            callback=MagicMock(),
            threaded=False,
            auto_ack=False,
            auto_declare=False
        )
        
        # Assert NO thread was created
        mock_thread_cls.assert_not_called()

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
        'threaded': True,  # <--- Important
        'callback': MagicMock(), # Successful callback
        'dlx_enable': False,
        'enable_retry_cycles': False
    }
    
    # Spy on _schedule_threadsafe to ensure it's called
    with patch.object(mock_consumer, '_schedule_threadsafe') as mock_schedule:
        mock_consumer._process_single_message(mock_method, mock_props, mock_body, runtime_config)
        
        # Verify ACK was scheduled safely
        # We expect _schedule_threadsafe to be called with (basic_ack, True, delivery_tag=99)
        mock_schedule.assert_called_with(
            mock_consumer._channel.basic_ack, 
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
        'threaded': False, # <--- Blocking
        'callback': MagicMock(),
        'dlx_enable': False
    }
    
    with patch.object(mock_consumer, '_schedule_threadsafe') as mock_schedule:
        mock_consumer._process_single_message(mock_method, mock_props, b'{}', runtime_config)
        
        # Verify schedule was called with threaded=False
        mock_schedule.assert_called_with(
            mock_consumer._channel.basic_ack, 
            False, 
            delivery_tag=99
        )
