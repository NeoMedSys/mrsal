import pytest
from unittest.mock import MagicMock
from mrsal.amqp.subclass import MrsalBlockingAMQP


class TestPassiveDeclarationAndQueueSettings:
    """Test passive declaration and new queue settings"""
    
    @pytest.fixture
    def mock_consumer(self):
        """Create a mock consumer with enhanced settings"""
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
            blocked_connection_timeout=60,
            # New default settings
            max_queue_length=10000,
            queue_overflow="drop-head",
            single_active_consumer=False,
            lazy_queue=False
        )
        
        # Mock connection and channel
        consumer._connection = MagicMock()
        consumer._channel = MagicMock()
        consumer.auto_declare_ok = True
        
        # Mock setup methods
        consumer.setup_blocking_connection = MagicMock()
        consumer._setup_exchange_and_queue = MagicMock()
        
        return consumer

    def test_passive_declaration_default_for_publisher(self, mock_consumer):
        """Test that publishers use passive declaration by default"""
        mock_consumer.publish_message(
            exchange_name="test_exch",
            routing_key="test_key",
            message="test_message",
            exchange_type="direct",
            queue_name="test_queue",
            auto_declare=True
            # passive=True should be default
        )

        # Publishers should ONLY pass basic params - no queue config!
        mock_consumer._setup_exchange_and_queue.assert_called_with(
            exchange_name="test_exch",
            queue_name="test_queue",
            exchange_type="direct",
            routing_key="test_key",
            passive=True  # Should be True by default for publishers
        )

    def test_passive_declaration_can_be_overridden(self, mock_consumer):
        """Test that passive can be explicitly set to False for publishers"""
        mock_consumer.publish_message(
            exchange_name="test_exch",
            routing_key="test_key",
            message="test_message",
            exchange_type="direct",
            queue_name="test_queue",
            auto_declare=True,
            passive=False  # Explicitly set to False
        )

        # Publishers should ONLY pass basic params - no queue config!
        mock_consumer._setup_exchange_and_queue.assert_called_with(
            exchange_name="test_exch",
            queue_name="test_queue",
            exchange_type="direct",
            routing_key="test_key",
            passive=False
        )

    def test_consumer_passive_declaration_default(self, mock_consumer):
        """Test that consumers use passive=False by default"""
        mock_consumer._channel.consume.return_value = []
        
        mock_consumer.start_consumer(
            queue_name="test_queue",
            auto_declare=True,
            exchange_name="test_exchange",
            exchange_type="direct",
            routing_key="test_key"
            # passive=False should be default for consumers
        )

        # Consumers DO handle queue config - verify all queue parameters are passed
        # The consumer should pass all its instance defaults + any explicit params
        mock_consumer._setup_exchange_and_queue.assert_called_with(
            exchange_name="test_exchange",
            queue_name="test_queue",
            exchange_type="direct",
            routing_key="test_key",
            dlx_enable=True,  # from instance
            dlx_exchange_name=None,
            dlx_routing_key=None,
            use_quorum_queues=True,  # from instance
            max_queue_length=None,  # None passed, so None used
            max_queue_length_bytes=None,
            queue_overflow=None,
            single_active_consumer=None,
            lazy_queue=None
        )
