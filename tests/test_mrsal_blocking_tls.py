import os
import unittest
from unittest.mock import patch
from pydantic import ValidationError

from mrsal.amqp.subclass import MrsalAMQP
from tests.conftest import SETUP_ARGS



class TestBlockRabbitSSLSetup(unittest.TestCase):

    def test_ssl_setup_with_valid_paths(self):
        with patch.dict('os.environ', {
            'RABBITMQ_CERT': 'test_cert.crt',
            'RABBITMQ_KEY': 'test_key.key',
            'RABBITMQ_CAFILE': 'test_ca.ca'
        }, clear=True):
            consumer = MrsalAMQP(**SETUP_ARGS, ssl=True, use_blocking=True)

            # Check if SSL paths are correctly loaded and blocking is used
            self.assertEqual(consumer.tls_dict['crt'], 'test_cert.crt')
            self.assertEqual(consumer.tls_dict['key'], 'test_key.key')
            self.assertEqual(consumer.tls_dict['ca'], 'test_ca.ca')

    @patch.dict('os.environ', {
        'RABBITMQ_CERT': '',
        'RABBITMQ_KEY': '',
        'RABBITMQ_CAFILE': ''
    })
    def test_ssl_setup_with_missing_paths(self):
        with self.assertRaises(ValidationError):
            MrsalAMQP(**SETUP_ARGS, ssl=True, use_blocking=True)

    @patch.dict(os.environ, {}, clear=True)
    def test_ssl_setup_without_env_vars(self):
        with self.assertRaises(ValidationError):
            MrsalAMQP(**SETUP_ARGS, ssl=True, use_blocking=True)


if __name__ == '__main__':
    unittest.main()
