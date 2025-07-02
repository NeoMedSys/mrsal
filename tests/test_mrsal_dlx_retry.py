import pytest
from unittest.mock import Mock, MagicMock, AsyncMock, patch
from pydantic.dataclasses import dataclass

from mrsal.amqp.subclass import MrsalBlockingAMQP, MrsalAsyncAMQP


@dataclass
class ExpectedPayload:
    id: int
    name: str
    active: bool


class AsyncIteratorMock:
    """Mock async iterator for aio-pika queue.iterator()"""
    def __init__(self, items):
        self.items = iter(items)
    
    def __aiter__(self):
        return self
    
    async def __anext__(self):
        try:
            return next(self.items)
        except StopIteration:
            raise StopAsyncIteration


class TestRetryMechanism:
    """Test retry mechanism with DLX cycles"""

    @pytest.fixture
    def mock_consumer(self):
        """Create a mock consumer with mocked connection and channel"""
        consumer = MrsalBlockingAMQP(
            host="localhost",
            port=5672,
            credentials=("user", "password"),
            virtual_host="testboi",
            ssl=False,
            verbose=False,
            prefetch_count=1,
            heartbeat=60,
            dlx_enable=True,
            max_retries=2,
            use_quorum_queues=True,
            blocked_connection_timeout=60
        )
        
        # Mock connection and channel
        consumer._connection = MagicMock()
        consumer._channel = MagicMock()
        consumer.auto_declare_ok = True
        
        # Mock setup methods
        consumer.setup_blocking_connection = MagicMock()
        consumer._setup_exchange_and_queue = MagicMock()
        
        return consumer

    def test_immediate_retry_params_exist(self, mock_consumer):
        """Test that start_consumer accepts immediate retry parameters"""
        # Should not raise error when passing immediate retry parameters
        mock_consumer._channel.consume.return_value = []
        
        try:
            mock_consumer.start_consumer(
                queue_name="test_queue",
                auto_declare=True,
                exchange_name="test_exchange",
                exchange_type="direct",
                routing_key="test_key",
                immediate_retry_delay=5,
                enable_retry_cycles=True,
                retry_cycle_interval=10,
                max_retry_time_limit=60
            )
        except Exception as e:
            if "unexpected keyword argument" in str(e):
                pytest.fail(f"start_consumer doesn't accept retry parameters: {e}")

    def test_retry_cycle_params_exist(self, mock_consumer):
        """Test that start_consumer accepts retry cycle parameters"""
        mock_consumer._channel.consume.return_value = []
        
        try:
            mock_consumer.start_consumer(
                queue_name="test_queue", 
                auto_declare=True,
                exchange_name="test_exchange",
                exchange_type="direct", 
                routing_key="test_key",
                enable_retry_cycles=True,
                retry_cycle_interval=15,
                max_retry_time_limit=90
            )
        except Exception as e:
            if "unexpected keyword argument" in str(e):
                pytest.fail(f"start_consumer doesn't accept retry cycle parameters: {e}")

    @patch('mrsal.amqp.subclass.time.sleep')
    def test_immediate_retry_delay(self, mock_sleep, mock_consumer):
        """Test that immediate retry delay is applied between retries"""
        mock_method_frame = MagicMock()
        mock_method_frame.delivery_tag = 123
        mock_properties = MagicMock()
        mock_properties.message_id = 'test_msg'
        invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'

        # Create generator that yields same message multiple times (simulating redelivery)
        def consume_generator():
            for _ in range(3):  # 3 attempts (initial + 2 retries)
                yield (mock_method_frame, mock_properties, invalid_body)

        mock_consumer._channel.consume.return_value = consume_generator()

        mock_consumer.start_consumer(
            queue_name="test_queue",
            callback=Mock(),
            auto_ack=False,
            auto_declare=True,
            exchange_name="test_exchange", 
            exchange_type="direct",
            routing_key="test_key",
            payload_model=ExpectedPayload,
            immediate_retry_delay=3  # 3 seconds delay
        )

        # Should have called sleep during retries
        mock_sleep.assert_called_with(3)

    def test_validation_failure_with_retry_cycles_disabled(self, mock_consumer):
        """Test that original DLX behavior is used when retry cycles disabled."""
        mock_method_frame = MagicMock()
        mock_method_frame.delivery_tag = 123
        mock_method_frame.routing_key = "test_key"
        mock_properties = MagicMock()
        mock_properties.message_id = 'test_msg'
        mock_properties.app_id = 'test_app'
        invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'

        # Create generator that simulates redelivery cycle
        def consume_generator():
            # Initial delivery + max_retries (2) = 3 total attempts
            for _ in range(3):
                yield (mock_method_frame, mock_properties, invalid_body)

        mock_consumer._channel.consume.return_value = consume_generator()

        mock_consumer.start_consumer(
            queue_name="test_queue",
            callback=Mock(),
            auto_ack=False,
            auto_declare=True,
            exchange_name="test_exchange",
            exchange_type="direct",
            routing_key="test_key",
            payload_model=ExpectedPayload,
            dlx_enable=True,
            enable_retry_cycles=False  # Disable retry cycles
        )

        # Should use original behavior (basic_nack with requeue=False) after max retries
        nack_calls = mock_consumer._channel.basic_nack.call_args_list
        
        # First calls should be requeue=True (immediate retries)
        for i in range(mock_consumer.max_retries):
            assert nack_calls[i][1]['requeue'] == True
        
        # Final call should be requeue=False (goes to DLX)
        final_nack_call = nack_calls[-1]
        assert final_nack_call[1]['delivery_tag'] == 123
        assert final_nack_call[1]['requeue'] == False  # Goes to DLX

    def test_validation_failure_with_retry_cycles_enabled(self, mock_consumer):
        """Test that retry cycle DLX logic is used when enabled"""
        mock_method_frame = MagicMock()
        mock_method_frame.delivery_tag = 123
        mock_method_frame.routing_key = "test_key"
        mock_properties = MagicMock()
        mock_properties.message_id = 'test_msg'
        mock_properties.app_id = 'test_app'
        mock_properties.headers = None
        invalid_body = b'{"id": "wrong_type", "name": "Test", "active": true}'

        # Create generator that simulates redelivery cycle
        def consume_generator():
            # Initial delivery + max_retries (2) = 3 total attempts
            for _ in range(3):
                yield (mock_method_frame, mock_properties, invalid_body)

        mock_consumer._channel.consume.return_value = consume_generator()

        # Mock the DLX publishing method
        mock_consumer._publish_to_dlx = MagicMock()

        mock_consumer.start_consumer(
            queue_name="test_queue",
            callback=Mock(),
            auto_ack=False,
            auto_declare=True,
            exchange_name="test_exchange",
            exchange_type="direct",
            routing_key="test_key",
            payload_model=ExpectedPayload,
            dlx_enable=True,
            enable_retry_cycles=True,  # Enable retry cycles
            retry_cycle_interval=10,
            max_retry_time_limit=60
        )

        # Should call the DLX retry cycle method instead of basic_nack
        mock_consumer._publish_to_dlx.assert_called()

    def test_callback_failure_triggers_retry(self, mock_consumer):
        """Test that callback failures trigger the retry mechanism"""
        mock_method_frame = MagicMock()
        mock_method_frame.delivery_tag = 456
        mock_properties = MagicMock()
        mock_properties.message_id = 'callback_test'
        valid_body = b'{"id": 123, "name": "Test", "active": true}'

        def consume_generator():
            for _ in range(3):
                yield (mock_method_frame, mock_properties, valid_body)

        mock_consumer._channel.consume.return_value = consume_generator()

        # Mock callback that raises exception
        failing_callback = Mock(side_effect=Exception("Callback failed"))

        mock_consumer.start_consumer(
            queue_name="test_queue",
            callback=failing_callback,
            auto_ack=False,
            auto_declare=True,
            exchange_name="test_exchange",
            exchange_type="direct",
            routing_key="test_key",
            payload_model=ExpectedPayload,
            immediate_retry_delay=1
        )

        # Callback should be called multiple times due to retries
        assert failing_callback.call_count >= 2
        
        # Should have nack calls with requeue
        nack_calls = mock_consumer._channel.basic_nack.call_args_list
        assert len(nack_calls) > 0
        assert nack_calls[0][1]['delivery_tag'] == 456

    def test_successful_processing_acks_message(self, mock_consumer):
        """Test that successful processing acknowledges the message"""
        mock_method_frame = MagicMock()
        mock_method_frame.delivery_tag = 789
        mock_properties = MagicMock()
        mock_properties.message_id = 'success_test'
        mock_properties.app_id = 'test_app'
        valid_body = b'{"id": 123, "name": "Test", "active": true}'

        mock_consumer._channel.consume.return_value = [
            (mock_method_frame, mock_properties, valid_body)
        ]

        successful_callback = Mock()  # No exception

        mock_consumer.start_consumer(
            queue_name="test_queue",
            callback=successful_callback,
            auto_ack=False,
            auto_declare=True,
            exchange_name="test_exchange",
            exchange_type="direct",
            routing_key="test_key",
            payload_model=ExpectedPayload
        )

        # Should acknowledge the message
        mock_consumer._channel.basic_ack.assert_called_with(delivery_tag=789)
        
        # Should not have any nack calls
        mock_consumer._channel.basic_nack.assert_not_called()


class TestAsyncRetryCycles:
    """Test async consumer retry cycle functionality"""

    @pytest.fixture
    def mock_async_consumer(self):
        """Create a mock async consumer"""
        consumer = MrsalAsyncAMQP(
            host="localhost",
            port=5672,
            credentials=("user", "password"),
            virtual_host="testboi",
            ssl=False,
            verbose=False,
            prefetch_count=1,
            heartbeat=60,
            dlx_enable=True,
            max_retries=2,
            use_quorum_queues=True
        )
        
        # Mock connection and channel
        consumer._connection = AsyncMock()
        consumer._channel = AsyncMock()
        consumer.auto_declare_ok = True
        
        # Mock setup methods
        consumer.setup_async_connection = AsyncMock()
        consumer._async_setup_exchange_and_queue = AsyncMock()
        
        return consumer

    @pytest.mark.asyncio
    async def test_async_start_consumer_has_retry_cycle_params(self, mock_async_consumer):
        """Test that async start_consumer accepts retry cycle parameters."""
        # Mock minimal setup
        mock_queue = AsyncMock()
        mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue
        
        # FIX: Create proper async iterator mock
        async_iterator = AsyncIteratorMock([])  # Empty iterator to avoid infinite loop
        
        # Mock the iterator method to return our async iterator (not a coroutine)
        mock_queue.iterator = Mock(return_value=async_iterator)

        # Should not raise error with retry cycle parameters
        await mock_async_consumer.start_consumer(
            queue_name="test_queue",
            exchange_name="test_exchange",
            exchange_type="direct",
            routing_key="test_key",
            enable_retry_cycles=True,
            retry_cycle_interval=15,
            max_retry_time_limit=90,
            immediate_retry_delay=5
        )

    @pytest.mark.asyncio
    @patch('mrsal.amqp.subclass.asyncio.sleep')
    async def test_async_immediate_retry_delay(self, mock_async_sleep, mock_async_consumer):
        """Test that immediate retry delay is applied in async consumer."""
        # Mock message that will fail processing
        mock_message = MagicMock()
        mock_message.delivery_tag = 123
        mock_message.app_id = 'test_app'
        mock_message.headers = None
        mock_message.body = b'{"id": "wrong_type", "name": "Test", "active": true}'
        mock_message.reject = AsyncMock()

        mock_properties = MagicMock()
        mock_properties.headers = None

        # Mock queue iterator to return message multiple times (simulating redelivery)
        mock_queue = AsyncMock()
        mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue
        
        # FIX: Create proper async iterator with multiple redeliveries
        async_iterator = AsyncIteratorMock([mock_message] * 3)  # 3 redeliveries
        mock_queue.iterator = Mock(return_value=async_iterator)

        await mock_async_consumer.start_consumer(
            queue_name="test_queue",
            callback=AsyncMock(side_effect=Exception("Processing failed")),
            auto_ack=False,
            auto_declare=True,
            exchange_name="test_exchange",
            exchange_type="direct",
            routing_key="test_key",
            immediate_retry_delay=2  # 2 seconds delay
        )

        # Verify asyncio.sleep was called with the delay
        mock_async_sleep.assert_called_with(2)

    @pytest.mark.asyncio
    async def test_async_validation_failure_with_cycles_disabled(self, mock_async_consumer):
        """Test async consumer with validation failure and retry cycles disabled"""
        # Mock message with invalid payload
        mock_message = MagicMock()
        mock_message.delivery_tag = 123
        mock_message.app_id = 'test_app'
        mock_message.headers = None
        mock_message.body = b'{"id": "wrong_type", "name": "Test", "active": true}'
        mock_message.reject = AsyncMock()
        mock_message.ack = AsyncMock()

        mock_properties = MagicMock()
        mock_properties.headers = None

        mock_queue = AsyncMock()
        mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue
        
        # Simulate multiple redeliveries
        async_iterator = AsyncIteratorMock([mock_message] * 3)
        mock_queue.iterator = Mock(return_value=async_iterator)

        await mock_async_consumer.start_consumer(
            queue_name="test_queue",
            callback=AsyncMock(),
            auto_ack=False,
            auto_declare=True,
            exchange_name="test_exchange",
            exchange_type="direct",
            routing_key="test_key",
            payload_model=ExpectedPayload,
            dlx_enable=True,
            enable_retry_cycles=False
        )

        # Should have called reject with requeue=False eventually
        mock_message.reject.assert_called()

    @pytest.mark.asyncio
    async def test_async_successful_processing(self, mock_async_consumer):
        """Test async consumer successful message processing"""
        # Mock message with valid payload
        mock_message = MagicMock()
        mock_message.delivery_tag = 456
        mock_message.app_id = 'test_app'
        mock_message.headers = None
        mock_message.body = b'{"id": 123, "name": "Test", "active": true}'
        mock_message.ack = AsyncMock()
        mock_message.reject = AsyncMock()

        mock_properties = MagicMock()
        mock_properties.headers = None

        mock_queue = AsyncMock()
        mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue
        
        # Single successful message
        async_iterator = AsyncIteratorMock([mock_message])
        mock_queue.iterator = Mock(return_value=async_iterator)

        successful_callback = AsyncMock()

        await mock_async_consumer.start_consumer(
            queue_name="test_queue",
            callback=successful_callback,
            auto_ack=False,
            auto_declare=True,
            exchange_name="test_exchange",
            exchange_type="direct",
            routing_key="test_key",
            payload_model=ExpectedPayload
        )

        # Should acknowledge the message
        mock_message.ack.assert_called()
        
        # Should not reject
        mock_message.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_async_callback_failure_retry_cycles(self, mock_async_consumer):
        """Test async consumer callback failure with retry cycles enabled"""
        mock_message = MagicMock()
        mock_message.delivery_tag = 789
        mock_message.app_id = 'test_app'
        mock_message.headers = None
        mock_message.body = b'{"id": 123, "name": "Test", "active": true}'
        mock_message.ack = AsyncMock()
        mock_message.reject = AsyncMock()

        mock_properties = MagicMock()
        mock_properties.headers = None

        mock_queue = AsyncMock()
        mock_async_consumer._async_setup_exchange_and_queue.return_value = mock_queue
        
        # Multiple message deliveries for retry testing
        async_iterator = AsyncIteratorMock([mock_message] * 3)
        mock_queue.iterator = Mock(return_value=async_iterator)

        # Mock the DLX publishing method
        mock_async_consumer._publish_to_dlx = AsyncMock()

        failing_callback = AsyncMock(side_effect=Exception("Async callback failed"))

        await mock_async_consumer.start_consumer(
            queue_name="test_queue",
            callback=failing_callback,
            auto_ack=False,
            auto_declare=True,
            exchange_name="test_exchange",
            exchange_type="direct",
            routing_key="test_key",
            payload_model=ExpectedPayload,
            dlx_enable=True,
            enable_retry_cycles=True,
            retry_cycle_interval=5,
            max_retry_time_limit=30
        )

        # Callback should be called multiple times
        assert failing_callback.call_count >= 2


class TestDLXRetryHeaders:
    """Test DLX retry cycle header management"""

    @pytest.fixture
    def mock_consumer(self):
        consumer = MrsalBlockingAMQP(
            host="localhost",
            port=5672,
            credentials=("user", "password"),
            virtual_host="testboi",
            dlx_enable=True,
            max_retries=2
        )
        consumer._connection = MagicMock()
        consumer._channel = MagicMock()
        return consumer

    def test_retry_cycle_info_extraction(self, mock_consumer):
        """Test extraction of retry cycle information from headers"""
        # Mock properties with retry headers
        mock_properties = MagicMock()
        mock_properties.headers = {
            'x-cycle-count': 2,
            'x-first-failure': '2024-01-01T10:00:00Z',
            'x-total-elapsed': 120000  # 2 minutes in ms
        }

        retry_info = mock_consumer._get_retry_cycle_info(mock_properties)
        
        assert retry_info['cycle_count'] == 2
        assert retry_info['first_failure'] == '2024-01-01T10:00:00Z'
        assert retry_info['total_elapsed'] == 120000

    def test_retry_cycle_info_defaults(self, mock_consumer):
        """Test default values when no retry headers present"""
        mock_properties = MagicMock()
        mock_properties.headers = None

        retry_info = mock_consumer._get_retry_cycle_info(mock_properties)
        
        assert retry_info['cycle_count'] == 0
        assert retry_info['first_failure'] is None
        assert retry_info['total_elapsed'] == 0

    def test_should_continue_retry_cycles_time_limit(self, mock_consumer):
        """Test retry cycle time limit checking"""
        # Within time limit
        retry_info = {'total_elapsed': 30000}  # 30 seconds
        should_continue = mock_consumer._should_continue_retry_cycles(
            retry_info, enable_retry_cycles=True, max_retry_time_limit=1  # 1 minute
        )
        assert should_continue is True

        # Exceeded time limit
        retry_info = {'total_elapsed': 120000}  # 2 minutes
        should_continue = mock_consumer._should_continue_retry_cycles(
            retry_info, enable_retry_cycles=True, max_retry_time_limit=1  # 1 minute
        )
        assert should_continue is False

    def test_should_continue_retry_cycles_disabled(self, mock_consumer):
        """Test retry cycle disabled"""
        retry_info = {'total_elapsed': 0}
        should_continue = mock_consumer._should_continue_retry_cycles(
            retry_info, enable_retry_cycles=False, max_retry_time_limit=60
        )
        assert should_continue is False

    def test_create_retry_cycle_headers(self, mock_consumer):
        """Test creation of retry cycle headers"""
        original_headers = {'custom-header': 'value'}
        
        headers = mock_consumer._create_retry_cycle_headers(
            original_headers=original_headers,
            cycle_count=1,
            first_failure='2024-01-01T10:00:00Z',
            processing_error='Test error',
            should_cycle=True,
            original_exchange='test_exchange',
            original_routing_key='test_key'
        )

        assert headers['x-cycle-count'] == 2  # cycle_count + 1
        assert headers['x-first-failure'] == '2024-01-01T10:00:00Z'
        assert headers['x-processing-error'] == 'Test error'
        assert headers['x-retry-exhausted'] is False
        assert headers['x-dead-letter-exchange'] == 'test_exchange'
        assert headers['x-dead-letter-routing-key'] == 'test_key'
        assert headers['custom-header'] == 'value'  # Original header preserved
