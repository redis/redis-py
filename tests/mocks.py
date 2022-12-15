# Various mocks for testing


class MockSocket:
    """
    A class simulating an readable socket, optionally raising a
    special exception every other read.
    """

    class TestError(BaseException):
        pass

    def __init__(self, data, interrupt_every=0):
        self.data = data
        self.counter = 0
        self.pos = 0
        self.interrupt_every = interrupt_every

    def tick(self):
        self.counter += 1
        if not self.interrupt_every:
            return
        if (self.counter % self.interrupt_every) == 0:
            raise self.TestError()

    def recv(self, bufsize):
        self.tick()
        bufsize = min(5, bufsize)  # truncate the read size
        result = self.data[self.pos : self.pos + bufsize]
        self.pos += len(result)
        return result

    def recv_into(self, buffer, nbytes=0, flags=0):
        self.tick()
        if nbytes == 0:
            nbytes = len(buffer)
        nbytes = min(5, nbytes)  # truncate the read size
        result = self.data[self.pos : self.pos + nbytes]
        self.pos += len(result)
        buffer[: len(result)] = result
        return len(result)
