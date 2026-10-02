class MrsalSetupError(Exception):
	"""Handling setup exceptions"""


class MrsalAbortedSetup(Exception):
	"""Handling abortion of the setup"""


class MrsalNoAsyncioLoopError(Exception):
	"""Handling no asyncio loop implemented"""


class MrsalDLXPublishTimeout(Exception):
	"""The async DLX publish did not complete within ``dlx_publish_timeout``"""


class MrsalConsumerCancelled(ConnectionError):
	"""The broker cancelled the async consumer (``Basic.Cancel``), e.g. its queue
	was deleted. A ``ConnectionError`` so ``start_consumer`` rebuilds it (#109)."""
