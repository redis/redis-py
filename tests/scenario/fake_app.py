import multiprocessing
import typing
from multiprocessing import Event as PEvent
from multiprocessing import Process
from threading import Event, Thread
from unittest.mock import patch

from redis import Redis


class FakeApp:

    def __init__(self, client: Redis, logic: typing.Callable[[Redis], None]):
        self.client = client
        self.logic = logic
        self.disconnects = 0

    def run(self) -> (Event, Thread):
        e = Event()
        t = Thread(target=self._run_logic, args=(e,))
        t.start()
        return e, t

    def _run_logic(self, e: Event):
        with patch.object(
            self.client, "_disconnect_raise", wraps=self.client._disconnect_raise
        ) as spy:
            while not e.is_set():
                self.logic(self.client)

            self.disconnects = spy.call_count


class FakeSubscriber:

    def __init__(self, client: Redis, logic: typing.Callable[[dict], None]):
        self.client = client
        self.logic = logic
        self.disconnects = multiprocessing.Value("i", 0)

    def run(self, channel: str) -> (PEvent, Process):
        e, started = PEvent(), PEvent()
        p = Process(target=self._run_logic, args=(e, started, channel))
        p.start()
        return e, started, p

    def _run_logic(self, should_stop: PEvent, started: PEvent, channel: str):
        pubsub = self.client.pubsub()

        with patch.object(
            pubsub, "_disconnect_raise_connect", wraps=pubsub._disconnect_raise_connect
        ) as spy_pubsub:
            pubsub.subscribe(channel)

            started.set()

            while not should_stop.is_set():
                message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)

                if message:
                    self.logic(message)

            self.disconnects.value = spy_pubsub.call_count
