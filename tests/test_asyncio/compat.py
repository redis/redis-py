import asyncio
from unittest import mock

try:
    mock.AsyncMock
except AttributeError:
    import mock


def create_task(coroutine):
    return asyncio.create_task(coroutine)
