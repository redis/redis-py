import sys

import pytest

from redis._parsers.resp2 import _RESP2Parser, _AsyncRESP2Parser
from redis._parsers.resp3 import _RESP3Parser, _AsyncRESP3Parser
from redis._parsers.socket import SocketBuffer


class FakeSocket:
    def __init__(self, payload: bytes):
        self.payload = payload

    def recv(self, n: int) -> bytes:
        if not self.payload:
            return b""
        chunk = self.payload[:n]
        self.payload = self.payload[n:]
        return chunk

    def settimeout(self, _timeout: float) -> None:
        pass


def make_resp2_parser(payload: bytes) -> _RESP2Parser:
    parser = _RESP2Parser(socket_read_size=65536)
    parser._buffer = SocketBuffer(
        FakeSocket(payload), socket_read_size=65536, socket_timeout=5
    )
    return parser


def make_resp3_parser(payload: bytes) -> _RESP3Parser:
    parser = _RESP3Parser(socket_read_size=65536)
    parser._buffer = SocketBuffer(
        FakeSocket(payload), socket_read_size=65536, socket_timeout=5
    )
    return parser


class TestRESP2DeepNesting:
    def test_nested_arrays_below_limit(self):
        depth = 100
        payload = (b"*1\r\n" * depth) + b"+OK\r\n"
        parser = make_resp2_parser(payload)
        result = parser._read_response(disable_decoding=True)
        expected = b"OK"
        for _ in range(depth):
            expected = [expected]
        assert result == expected

    def test_nested_arrays_above_default_limit(self):
        depth = sys.getrecursionlimit() + 200
        payload = (b"*1\r\n" * depth) + b"+OK\r\n"
        parser = make_resp2_parser(payload)
        result = parser._read_response(disable_decoding=True)
        expected = b"OK"
        for _ in range(depth):
            expected = [expected]
        assert result == expected

    def test_empty_arrays(self):
        payload = b"*0\r\n"
        parser = make_resp2_parser(payload)
        result = parser._read_response(disable_decoding=True)
        assert result == []

    def test_null_array(self):
        payload = b"*-1\r\n"
        parser = make_resp2_parser(payload)
        result = parser._read_response(disable_decoding=True)
        assert result is None

    def test_multiple_elements_nested(self):
        payload = b"*2\r\n*2\r\n+OK\r\n+OK\r\n+OK\r\n"
        parser = make_resp2_parser(payload)
        result = parser._read_response(disable_decoding=True)
        assert result == [[b"OK", b"OK"], b"OK"]

    def test_deeply_nested_multiple_elements(self):
        depth = 500
        payload = (b"*1\r\n" * depth) + b"*2\r\n+OK\r\n+OK\r\n"
        parser = make_resp2_parser(payload)
        result = parser._read_response(disable_decoding=True)
        expected = [b"OK", b"OK"]
        for _ in range(depth):
            expected = [expected]
        assert result == expected

    def test_mixed_types_nested(self):
        payload = b"*3\r\n+OK\r\n:42\r\n$5\r\nhello\r\n"
        parser = make_resp2_parser(payload)
        result = parser._read_response(disable_decoding=True)
        assert result == [b"OK", 42, b"hello"]


class TestRESP3DeepNesting:
    def test_nested_arrays(self):
        depth = sys.getrecursionlimit() + 200
        payload = (b"*1\r\n" * depth) + b"+OK\r\n"
        parser = make_resp3_parser(payload)
        result = parser._read_response(disable_decoding=True)
        expected = b"OK"
        for _ in range(depth):
            expected = [expected]
        assert result == expected

    def test_nested_sets(self):
        depth = 200
        payload = (b"~1\r\n" * depth) + b"+OK\r\n"
        parser = make_resp3_parser(payload)
        result = parser._read_response(disable_decoding=True)
        expected = b"OK"
        for _ in range(depth):
            expected = [expected]
        assert result == expected

    def test_nested_maps(self):
        depth = 200
        payload = (b"%1\r\n+key\r\n" * depth) + b"+value\r\n"
        parser = make_resp3_parser(payload)
        result = parser._read_response(disable_decoding=True)
        expected = b"value"
        for _ in range(depth):
            expected = {b"key": expected}
        assert result == expected

    def test_mixed_nested_aggregates(self):
        payload = b"*1\r\n~1\r\n%1\r\n+key\r\n+value\r\n"
        parser = make_resp3_parser(payload)
        result = parser._read_response(disable_decoding=True)
        assert result == [[{b"key": b"value"}]]

    def test_empty_set(self):
        payload = b"~0\r\n"
        parser = make_resp3_parser(payload)
        result = parser._read_response(disable_decoding=True)
        assert result == []

    def test_empty_map(self):
        payload = b"%0\r\n"
        parser = make_resp3_parser(payload)
        result = parser._read_response(disable_decoding=True)
        assert result == {}
