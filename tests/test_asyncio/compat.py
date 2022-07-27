import asyncio
import sys
from unittest import mock

try:
    mock.AsyncMock
except AttributeError:
    import mock


def create_task(coroutine):
    if sys.version_info[:2] >= (3, 7):
        return asyncio.create_task(coroutine)
    else:
        return asyncio.ensure_future(coroutine)
