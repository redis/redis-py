# Helper Mocking classes for the tests.
from anyio import EndOfStream
from anyio.abc import ByteReceiveStream
from anyio.streams.buffered import BufferedByteReceiveStream


class MockMemoryByteStreamReceiver(ByteReceiveStream):
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    async def receive(self, max_bytes: int = 65536) -> bytes:
        result = self.data[self.pos : self.pos + max_bytes]
        self.pos += len(result)
        if result:
            return result

        raise EndOfStream

    async def aclose(self) -> None:
        self.pos = len(self.data)


class MockStream(BufferedByteReceiveStream):
    """
    A class simulating an AnyIO input buffer, optionally raising a
    special exception every other read.
    """

    class TestError(BaseException):
        pass

    def __init__(self, data: bytes, interrupt_every: int = 0):
        super().__init__(MockMemoryByteStreamReceiver(data))
        self.counter = 0
        self.interrupt_every = interrupt_every

    def tick(self):
        self.counter += 1
        if not self.interrupt_every:
            return
        if (self.counter % self.interrupt_every) == 0:
            raise self.TestError()

    async def receive(self, max_bytes: int = 65536) -> bytes:
        self.tick()
        data = await super().receive(5)
        # print(f"receive() returned {data!r}")
        return data

    async def receive_exactly(self, nbytes: int) -> bytes:
        # print(f"receive_exactly({nbytes}) called")
        self.tick()
        data = await super().receive_exactly(nbytes)
        # print(f"  returned {data!r}")
        return data

    async def receive_until(self, delimiter: bytes, max_bytes: int) -> bytes:
        # print(f"receive_until({delimiter!r}) called")
        self.tick()
        data = await super().receive_until(delimiter, max_bytes)
        # print(f"  returned {data!r}")
        return data
