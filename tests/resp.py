import itertools
from contextlib import closing
from types import NoneType
from typing import Any, Generator, List, Optional, Tuple, Union

CRNL = b"\r\n"


class VerbatimString(bytes):
    """
    A string that is encoded as a resp3 verbatim string
    """

    def __new__(cls, value: bytes, hint: str) -> "VerbatimString":
        return bytes.__new__(cls, value)

    def __init__(self, value: bytes, hint: str) -> None:
        self.hint = hint

    def __repr__(self) -> str:
        return f"VerbatimString({super().__repr__()}, {self.hint!r})"


class PushData(list):
    """
    A special type of list indicating data from a push response
    """

    def __repr__(self) -> str:
        return f"PushData({super().__repr__()})"


class RespEncoder:
    """
    A class for simple RESP protocol encoding for unit tests
    """

    def __init__(self, protocol: int = 2, encoding: str = "utf-8") -> None:
        self.protocol = protocol
        self.encoding = encoding

    def encode(self, data: Any, hint: Optional[str] = None) -> bytes:
        if isinstance(data, dict):
            if self.protocol > 2:
                result = f"%{len(data)}\r\n".encode()
                for key, val in data.items():
                    result += self.encode(key) + self.encode(val)
                return result
            else:
                # Automatically encode dicts as flattened key, value arrays
                mylist = list(
                    itertools.chain(*((key, val) for (key, val) in data.items()))
                )
                return self.encode(mylist)

        elif isinstance(data, list):
            if isinstance(data, PushData) and self.protocol > 2:
                result = f">{len(data)}\r\n".encode()
            else:
                result = f"*{len(data)}\r\n".encode()
            for val in data:
                result += self.encode(val)
            return result

        elif isinstance(data, set):
            if self.protocol > 2:
                result = f"~{len(data)}\r\n".encode()
                for val in data:
                    result += self.encode(val)
                return result
            else:
                return self.encode(list(data))

        elif isinstance(data, str):
            enc = data.encode(self.encoding)
            # long strings or strings with control characters must be encoded as bulk
            # strings
            if hint or len(enc) > 20 or b"\r" in enc or b"\n" in enc:
                return self.encode_bulkstr(enc, hint)
            return b"+" + enc + b"\r\n"

        elif isinstance(data, bytes):
            return self.encode_bulkstr(data, hint)

        elif isinstance(data, bool):
            if self.protocol == 2:
                return b":1\r\n" if data else b":0\r\n"
            return b"t\r\n" if data else b"f\r\n"

        elif isinstance(data, int):
            if (data > 2**63 - 1) or (data < -(2**63)):
                if self.protocol > 2:
                    return f"({data}\r\n".encode()  # resp3 big int
                return f"+{data}\r\n".encode()  # force to simple string
            return f":{data}\r\n".encode()
        elif isinstance(data, float):
            if self.protocol > 2:
                return f",{data}\r\n".encode()  # resp3 double
            return f"+{data}\r\n".encode()  # simple string

        elif isinstance(data, NoneType):
            if self.protocol > 2:
                return b"_\r\n"  # resp3 null
            return b"$-1\r\n"  # Null bulk string
            # some commands return null array: b"*-1\r\n"

        else:
            raise NotImplementedError(f"encode not implemented for {type(data)}")

    def encode_bulkstr(self, bstr: bytes, hint: Optional[str]) -> bytes:
        if self.protocol > 2 and hint is not None:
            # a resp3 verbatim string
            return f"={len(bstr)}\r\n{hint}:".encode() + bstr + b"\r\n"
        # regular bulk string
        return f"${len(bstr)}\r\n".encode() + bstr + b"\r\n"


def encode(value: Any, protocol: int = 2, hint: Optional[str] = None) -> bytes:
    """
    Encode a value using the RESP protocol
    """
    return RespEncoder(protocol).encode(value, hint)


# a stateful RESP parser implemented via a generator
def resp_parse(
    buffer: bytes,
) -> Generator[Optional[Tuple[Any, bytes]], Union[None, bytes], None]:
    """
    A stateful, generator based, RESP parser.
    Returns a generator producing at most a single top-level primitive.
    Yields tuple of (data_item, unparsed), or None if more data is needed.
    It is fed more data with generator.send()
    """
    # Read the first line of resp or yield to get more data
    while CRNL not in buffer:
        incoming = yield None
        assert incoming is not None
        buffer += incoming
    cmd, rest = buffer.split(CRNL, 1)

    code, arg = cmd[:1], cmd[1:]

    if code == b":" or code == b"(":  # integer, resp3 large int
        yield int(arg), rest

    elif code == b"t":  # resp3 true
        yield True, rest

    elif code == b"f":  # resp3 false
        yield False, rest

    elif code == b"_":  # resp3 null
        yield None, rest

    elif code == b",":  # resp3 double
        yield float(arg), rest

    elif code == b"+":  # simple string
        # we decode them automatically
        yield arg.decode(), rest

    elif code == b"$":  # bulk string
        count = int(arg)
        expect = count + 2  # +2 for the trailing CRNL
        while len(rest) < expect:
            incoming = yield (None)
            assert incoming is not None
            rest += incoming
        bulkstr = rest[:count]
        # bulk strings are not decoded, could contain binary data
        yield bulkstr, rest[expect:]

    elif code == b"=":  # verbatim strings
        count = int(arg)
        expect = count + 4 + 2  # 4 type and colon +2 for the trailing CRNL
        while len(rest) < expect:
            incoming = yield (None)
            assert incoming is not None
            rest += incoming
        hint = rest[:3]
        result = rest[4 : (count + 4)]
        # verbatim strings are not decoded, could contain binary data
        yield VerbatimString(result, hint.decode()), rest[expect:]

    elif code in b"*>":  # array or push data
        count = int(arg)
        result_array = []
        for _ in range(count):
            # recursively parse the next array item
            with closing(resp_parse(rest)) as parser:
                parsed = parser.send(None)
                while parsed is None:
                    incoming = yield None
                    parsed = parser.send(incoming)
            value, rest = parsed
            result_array.append(value)
        if code == b">":
            yield PushData(result_array), rest
        else:
            yield result_array, rest

    elif code == b"~":  # set
        count = int(arg)
        result_set = set()
        for _ in range(count):
            # recursively parse the next set item
            with closing(resp_parse(rest)) as parser:
                parsed = parser.send(None)
                while parsed is None:
                    incoming = yield None
                    parsed = parser.send(incoming)
            value, rest = parsed
            result_set.add(value)
        yield result_set, rest

    elif code == b"%":  # map
        count = int(arg)
        result_map = {}
        for _ in range(count):
            # recursively parse the next key, and value
            with closing(resp_parse(rest)) as parser:
                parsed = parser.send(None)
                while parsed is None:
                    incoming = yield None
                    parsed = parser.send(incoming)
            key, rest = parsed
            with closing(resp_parse(rest)) as parser:
                parsed = parser.send(None)
                while parsed is None:
                    incoming = yield None
                    parsed = parser.send(incoming)
            value, rest = parsed
            result_map[key] = value
        yield result_map, rest
    else:
        if code in b"-!":
            raise NotImplementedError(f"resp opcode '{code.decode()}' not implemented")
        raise ValueError(f"Unknown opcode '{code.decode()}'")


class NeedMoreData(RuntimeError):
    """
    Raised when more data is needed to complete a parse
    """


class RespParser:
    """
    A class for simple RESP protocol decoding for unit tests
    """

    def __init__(self) -> None:
        self.parser: Optional[
            Generator[Optional[Tuple[Any, bytes]], Union[None, bytes], None]
        ] = None
        # which has not resulted in a parsed value
        self.consumed: List[bytes] = []

    def parse(self, buffer: bytes) -> Optional[Any]:
        """
        Parse a buffer of data, return a tuple of a single top-level primitive and the
        remaining buffer or raise NeedMoreData if more data is needed
        """
        if self.parser is None:
            # create a new parser generator, initializing it with
            # any unparsed data from previous calls
            buffer = b"".join(self.consumed) + buffer
            del self.consumed[:]
            self.parser = resp_parse(buffer)
            parsed = self.parser.send(None)
        else:
            # sen more data to the parser
            parsed = self.parser.send(buffer)

        if parsed is None:
            self.consumed.append(buffer)
            raise NeedMoreData()

        # got a value, close the parser, store the remaining buffer
        self.parser.close()
        self.parser = None
        value, remaining = parsed
        self.consumed = [remaining]
        return value

    def get_unparsed(self) -> bytes:
        return b"".join(self.consumed)

    def close(self) -> None:
        if self.parser is not None:
            self.parser.close()
            self.parser = None
        del self.consumed[:]


def parse_all(buffer: bytes) -> Tuple[List[Any], bytes]:
    """
    Parse all the data in the buffer, returning the list of top-level objects and the
    remaining buffer
    """
    with closing(RespParser()) as parser:
        result: List[Any] = []
        while True:
            try:
                result.append(parser.parse(buffer))
                buffer = b""
            except NeedMoreData:
                return result, parser.get_unparsed()


def parse_chunks(buffers: List[bytes]) -> Tuple[List[Any], bytes]:
    """
    Parse all the data in the buffers, returning the list of top-level objects and the
    remaining buffer.
    Used primarily for testing, since it will parse the data in chunks
    """
    result: List[Any] = []
    with closing(RespParser()) as parser:
        for buffer in buffers:
            while True:
                try:
                    result.append(parser.parse(buffer))
                    buffer = b""
                except NeedMoreData:
                    break
        return result, parser.get_unparsed()
