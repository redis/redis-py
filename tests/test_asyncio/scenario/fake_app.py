import typing
from asyncio import Event, Task, create_task
from unittest.mock import patch

from redis.asyncio import Redis


class AsyncFakeApp:

    def __init__(
        self, client: Redis, logic: typing.Callable[[Redis], typing.Awaitable[None]]
    ):
        self.client = client
        self.logic = logic
        self.disconnects = 0

    async def run(self):
        e = Event()
        t = create_task(self._run_logic(e))
        return e, t

    async def _run_logic(self, e: Event):
        with patch.object(
            self.client, "_disconnect_raise", wraps=self.client._disconnect_raise
        ) as spy:
            while not e.is_set():
                await self.logic(self.client)

            self.disconnects = spy.call_count


class AsyncFakeSubscriber:

    def __init__(
        self, client: Redis, logic: typing.Callable[[dict], typing.Awaitable[None]]
    ):
        self.client = client
        self.logic = logic
        self.disconnects = 0

    async def run(self, channel: str) -> (Event, Task):
        e = Event()
        t = create_task(self._run_logic(e, channel))
        return e, t

    async def _run_logic(self, should_stop: Event, channel: str):
        pubsub = self.client.pubsub()

        with patch.object(
            pubsub, "_disconnect_raise_connect", wraps=pubsub._disconnect_raise_connect
        ) as spy_pubsub:
            await pubsub.subscribe(channel)

            while not should_stop.is_set():
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1
                )

                if message:
                    await self.logic(message)

            self.disconnects = spy_pubsub.call_count
