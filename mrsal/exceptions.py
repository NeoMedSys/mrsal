class MrsalSetupError(Exception):
    """Handling setup exceptions"""


class MrsalAbortedSetup(Exception):
    """Handling abortion of the setup"""


class MrsalNoAsyncioLoopError(Exception):
    """Handling no asyncio loop implemented"""
