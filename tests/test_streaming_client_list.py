"""
Unit tests for the streaming CLIENT LIST prototype: SocketBuffer.read_bulk_lines()
and Connection.read_response_lines_streaming(). These use a fake socket so they
don't require a live Redis server, and specifically exercise chunk-boundary
behavior (lines split across arbitrary recv() boundaries) since that's the part
of the design most likely to have off-by-one bugs.
"""

import threading
import time
import weakref

import pytest

# tracemalloc requires a CPython-specific memory allocator hook
# (_tracemalloc) that PyPy does not provide - imported lazily, only by
# the two tests that actually need it (via pytest.importorskip), rather
# than unconditionally at module level, so a PyPy run skips just those
# two tests instead of failing to collect this entire file.

from redis._parsers.hiredis import _HiredisParser
from redis._parsers.helpers import parse_client_list_line
from redis._parsers.socket import SERVER_CLOSED_CONNECTION_ERROR, SocketBuffer
from redis.connection import Connection
from redis.exceptions import ConnectionError, ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.utils import HIREDIS_AVAILABLE


class FakeSocket:
    """A socket stand-in that yields pre-chunked bytes from recv()."""

    def __init__(self, chunks):
        self._chunks = list(chunks)

    def recv(self, bufsize):
        if not self._chunks:
            return b""
        chunk = self._chunks.pop(0)
        if len(chunk) > bufsize:
            # split it up like a real socket would if the caller asked
            # for less than what's available
            self._chunks.insert(0, chunk[bufsize:])
            chunk = chunk[:bufsize]
        return chunk

    def settimeout(self, value):
        pass

    def sendall(self, data):
        # lets this double as conn._sock so Connection.send_command()
        # doesn't try to open a REAL socket to a real server
        pass


class TimeoutInjectingSocket(FakeSocket):
    """Like FakeSocket, but raises socket.timeout once all chunks are used."""

    def recv(self, bufsize):
        if not self._chunks:
            import socket as socket_module

            raise socket_module.timeout("timed out")
        return super().recv(bufsize)


CRLF = b"\r\n"


def make_reply(num_clients):
    """Build a raw RESP bulk-string CLIENT LIST reply for num_clients clients."""
    lines = [
        f"id={i} addr=127.0.0.1:{10000 + i} name=".encode() for i in range(num_clients)
    ]
    payload = b"\n".join(lines) + (b"\n" if lines else b"")
    header = b"$%d" % len(payload)
    return header + CRLF + payload + CRLF, header, len(payload)


def chunk_bytes(data, chunk_size):
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 7, 16, 4096])
@pytest.mark.parametrize("num_clients", [0, 1, 2, 50])
def test_read_bulk_lines_matches_expected_across_chunk_sizes(chunk_size, num_clients):
    reply, expected_header, expected_length = make_reply(num_clients)
    sock = FakeSocket(chunk_bytes(reply, chunk_size))
    buffer = SocketBuffer(sock, socket_read_size=chunk_size, socket_timeout=1)

    header = buffer.readline()
    assert header == expected_header
    length = int(header[1:])
    assert length == expected_length

    lines = list(buffer.read_bulk_lines(length))
    assert len(lines) == num_clients
    for i, line in enumerate(lines):
        parsed = parse_client_list_line(line)
        assert parsed["id"] == str(i)

    # trailing CRLF must be fully consumed, buffer left clean for next reply
    assert buffer.unread_bytes() == 0


def test_read_bulk_lines_leaves_next_reply_untouched():
    reply, _, _ = make_reply(3)
    reply += b"+OK\r\n"
    sock = FakeSocket(chunk_bytes(reply, 5))
    buffer = SocketBuffer(sock, socket_read_size=5, socket_timeout=1)

    header = buffer.readline()
    length = int(header[1:])
    lines = list(buffer.read_bulk_lines(length))
    assert len(lines) == 3

    # the next reply on the wire must be untouched by the streaming read
    assert buffer.readline() == b"+OK"


def test_read_bulk_lines_yields_final_line_with_no_trailing_newline():
    # make_reply() always appends a trailing "\n" after the last record
    # (matching real server behavior), so this exercises the OTHER
    # fallback branch on its own: a payload whose very last "line" has
    # no newline terminator at all before `length` bytes are exhausted.
    payload = b"id=0 addr=x:1\nid=1 addr=x:2"  # no trailing \n
    reply = b"$%d\r\n%s\r\n" % (len(payload), payload)
    sock = FakeSocket(chunk_bytes(reply, 4))
    buffer = SocketBuffer(sock, socket_read_size=4, socket_timeout=1)

    header = buffer.readline()
    length = int(header[1:])
    lines = list(buffer.read_bulk_lines(length))
    assert lines == [b"id=0 addr=x:1", b"id=1 addr=x:2"]
    assert buffer.unread_bytes() == 0


def test_read_bulk_lines_bounds_memory():
    """
    The whole point of this feature: peak memory actually allocated while
    draining the reply must stay close to socket_read_size, not grow
    anywhere near the size of the full (multi-hundred-KB) reply.

    This uses tracemalloc rather than SocketBuffer.unread_bytes(), because
    unread_bytes() only reflects SocketBuffer's own leftover BytesIO - the
    `pending` accumulator inside read_bulk_lines() is filled via direct
    socket recv() calls that never touch that buffer, so a regression
    reintroducing full-reply buffering into `pending` would leave
    unread_bytes() near zero throughout and pass unnoticed.

    Measures the DELTA against a baseline taken just before the loop,
    not tracemalloc's absolute traced-memory total - conftest.py's own
    session-scoped, autouse `enable_tracemalloc` fixture already calls
    tracemalloc.start() for the whole test session, so by the time this
    test runs, `get_traced_memory()` already reflects everything traced
    across every EARLIER test in the same session, not just this one.
    Calling tracemalloc.start()/stop() again here, as an earlier version
    of this test did, is itself wrong twice over: start() while already
    tracing does not reset the accumulated totals (so the "baseline" was
    never actually zero), and stop() would prematurely end tracing for
    every later test in the session.
    """
    tracemalloc = pytest.importorskip("tracemalloc")
    num_clients = 20_000
    reply, _, _ = make_reply(num_clients)
    socket_read_size = 4096
    sock = FakeSocket(chunk_bytes(reply, socket_read_size))
    buffer = SocketBuffer(sock, socket_read_size=socket_read_size, socket_timeout=1)

    header = buffer.readline()
    length = int(header[1:])

    baseline, _ = tracemalloc.get_traced_memory()
    peak_delta = 0
    for _ in buffer.read_bulk_lines(length):
        current, _ = tracemalloc.get_traced_memory()
        peak_delta = max(peak_delta, current - baseline)
    peak_current = peak_delta

    # peak traced allocation must stay a small multiple of socket_read_size,
    # nowhere near the full multi-hundred-KB reply size
    assert peak_current < socket_read_size * 20
    assert len(reply) > socket_read_size * 10  # sanity check the test is meaningful


def test_read_bulk_lines_stays_linear_time_with_tiny_chunks():
    # a round-27 review found read_bulk_lines() was algorithmically
    # quadratic, not linear, in the size of a single unterminated
    # record when a peer (malicious/compromised server, or an on-path
    # attacker) delivers it back a few bytes at a time instead of in
    # normal-sized chunks: `pending += chunk` on a plain `bytes` object
    # has no in-place growth (fixed by switching to a bytearray), AND
    # (a deeper issue the first fix alone did not address) `b"\n" in
    # pending` re-scanned the ENTIRE growing accumulator on every
    # single iteration, regardless of chunk size - repeated at O(chunk
    # count) times, that is quadratic in the record's total size for
    # small chunks. Verified directly: with the pre-fix code and 1-byte
    # chunks, accumulating a few hundred KB took minutes, not seconds -
    # directly undermining MAX_UNTERMINATED_LINE_SIZE's whole purpose of
    # bounding the damage from a pathological record, since the safety
    # valve wouldn't fire until an already-enormous amount of CPU time
    # had been burned getting there. Fixed by searching for '\n' only
    # within each newly-arrived chunk (bounded by that chunk's own
    # size), relying on the loop's invariant that `pending` itself never
    # contains '\n' at the top of the loop.
    #
    # This uses a real time bound (generous, but tight enough that the
    # old quadratic behavior - which took minutes for even 200,000
    # bytes - would fail it) rather than just re-asserting correctness,
    # since the bug was purely about HOW LONG this takes, not WHAT it
    # returns.
    tracemalloc = pytest.importorskip("tracemalloc")

    class _OneByteAtATimeSocket:
        def __init__(self, data):
            self._data = data
            self._pos = 0

        def recv(self, bufsize):
            if self._pos >= len(self._data):
                return b""
            byte = self._data[self._pos : self._pos + 1]
            self._pos += 1
            return byte

    content = b"x" * 1_500_000
    payload = content + b"\n"
    sock = _OneByteAtATimeSocket(payload + b"\r\n")
    buffer = SocketBuffer(sock, socket_read_size=1, socket_timeout=1)
    buffer.MAX_UNTERMINATED_LINE_SIZE = len(content) + 10

    # conftest.py's own session-scoped, autouse `enable_tracemalloc`
    # fixture leaves tracemalloc running for the whole test session -
    # its PER-ALLOCATION overhead alone, applied 1.5 million times here,
    # dominates the measurement (confirmed directly: ~9-12s under
    # tracemalloc vs ~1s without it for the exact same call), swamping
    # the very algorithmic difference this test exists to detect.
    # Suspending tracing for just this timing-critical section (and
    # restoring it in the `finally`, for whatever later test in the
    # session might still want it) measures the real cost instead.
    was_tracing = tracemalloc.is_tracing()
    if was_tracing:
        tracemalloc.stop()
    try:
        start = time.monotonic()
        lines = list(buffer.read_bulk_lines(len(payload)))
        elapsed = time.monotonic() - start
    finally:
        if was_tracing:
            tracemalloc.start()

    assert lines == [content]
    # Calibrated directly against both versions at this exact size: the
    # fix takes ~1s here; a standalone reproduction of the pre-fix
    # whole-buffer-rescan bug takes ~15s - comfortably separated by this
    # bound either way, not a hair-trigger threshold.
    assert elapsed < 8.0, (
        f"read_bulk_lines() took {elapsed:.1f}s for a 1,500,000-byte "
        "record delivered one byte at a time - the pre-fix quadratic-"
        "time bug took ~15s at this exact size; the fix takes ~1s"
    )


def test_read_response_lines_streaming_rejects_hiredis():
    if not HIREDIS_AVAILABLE:
        pytest.skip("hiredis not installed")
    conn = Connection(parser_class=_HiredisParser)
    conn._parser = _HiredisParser(socket_read_size=4096)
    with pytest.raises(NotImplementedError):
        list(conn.read_response_lines_streaming())


def test_read_response_lines_streaming_propagates_error_reply():
    from redis._parsers.resp2 import _RESP2Parser

    conn = Connection()
    parser = _RESP2Parser(socket_read_size=4096)
    error_reply = b"-ERR something went wrong\r\n"
    parser._buffer = SocketBuffer(
        FakeSocket(chunk_bytes(error_reply, 4)), socket_read_size=4, socket_timeout=1
    )
    conn._parser = parser
    with pytest.raises(ResponseError):
        list(conn.read_response_lines_streaming(disconnect_on_error=False))


def test_read_response_lines_streaming_empty_reply_yields_nothing():
    from redis._parsers.resp2 import _RESP2Parser

    conn = Connection()
    parser = _RESP2Parser(socket_read_size=4096)
    parser._buffer = SocketBuffer(
        FakeSocket([b"$0\r\n\r\n"]), socket_read_size=4096, socket_timeout=1
    )
    conn._parser = parser
    assert list(conn.read_response_lines_streaming()) == []


def test_read_response_lines_streaming_handles_resp3_null():
    # RESP3's canonical null type ("_\r\n") - _RESP3Parser._read_response()
    # returns None for this; the streaming reader must likewise yield
    # nothing rather than fall through to raise InvalidResponse.
    conn, _parser = _make_resp3_connection(chunk_bytes(b"_\r\n", 2))
    assert list(conn.read_response_lines_streaming()) == []


def test_read_response_lines_streaming_handles_null_bulk_string():
    # a round-24 review found the RESP2 (and RESP3-shared) null bulk-
    # string reply ("$-1\r\n") had no direct test of its own - only its
    # RESP3-only sibling, the canonical null ("_\r\n") above, did.
    conn = _make_resp2_connection(chunk_bytes(b"$-1\r\n", 2))
    assert list(conn.read_response_lines_streaming()) == []


def test_recv_capped_raises_on_closed_socket():
    sock = FakeSocket([])
    buffer = SocketBuffer(sock, socket_read_size=16, socket_timeout=1)
    with pytest.raises(ConnectionError, match=SERVER_CLOSED_CONNECTION_ERROR):
        buffer._recv_capped(16)


def _make_resp2_connection(chunks, socket_read_size=64):
    from redis._parsers.resp2 import _RESP2Parser

    conn = Connection()
    fake_sock = FakeSocket(chunks)
    # Connection.send_command() only skips its real connect() when
    # self._sock is already truthy - wire it to the SAME fake socket the
    # buffer reads from, so tests that go through the public
    # client_list_iter() API (which calls send_command()) don't silently
    # open a real socket to a real server and discard this fake data.
    conn._sock = fake_sock
    parser = _RESP2Parser(socket_read_size=socket_read_size)
    parser._buffer = SocketBuffer(
        fake_sock, socket_read_size=socket_read_size, socket_timeout=1
    )
    conn._parser = parser
    return conn


def _make_resp3_connection(chunks, socket_read_size=8):
    from redis._parsers.encoders import Encoder
    from redis._parsers.resp3 import _RESP3Parser

    conn = Connection()
    fake_sock = FakeSocket(chunks)
    conn._sock = fake_sock
    parser = _RESP3Parser(socket_read_size=socket_read_size)
    parser._buffer = SocketBuffer(
        fake_sock, socket_read_size=socket_read_size, socket_timeout=1
    )
    # normally set by on_connect(); push-frame decoding needs it. Using
    # decode_responses=True here exercises the round-2 fix: push elements
    # must honor the connection's decoding config, not always stay bytes.
    parser.encoder = Encoder(
        encoding="utf-8", encoding_errors="strict", decode_responses=True
    )
    conn._parser = parser
    return conn, parser


def test_read_response_lines_streaming_ordinary_error_does_not_disconnect():
    # an application-level error (e.g. NOPERM, a bad argument) must not
    # tear down an otherwise healthy connection - matching read_response()
    conn = _make_resp2_connection(chunk_bytes(b"-ERR bad argument\r\n", 4))
    disconnected = []
    conn.disconnect = lambda: disconnected.append(True)
    with pytest.raises(ResponseError):
        list(conn.read_response_lines_streaming())  # default disconnect_on_error=True
    assert disconnected == []


def test_read_response_lines_streaming_connection_error_does_disconnect():
    from redis.exceptions import ConnectionError as RedisConnectionError

    conn = _make_resp2_connection(
        chunk_bytes(b"-ERR max number of clients reached\r\n", 4)
    )
    disconnected = []
    conn.disconnect = lambda: disconnected.append(True)
    with pytest.raises(RedisConnectionError):
        list(conn.read_response_lines_streaming())
    assert disconnected == [True]


def test_read_response_lines_streaming_handles_resp3_blob_error():
    # RESP3 blob errors ("!<len>\r\n<text>\r\n") are a distinct encoding
    # from simple errors ("-<text>\r\n") for the same class of ordinary
    # application error - must decode like one and not disconnect.
    error_text = b"NOPERM this user has no permissions"
    blob_error = b"!%d\r\n%s\r\n" % (len(error_text), error_text)
    conn, _parser = _make_resp3_connection(chunk_bytes(blob_error, 5))
    disconnected = []
    conn.disconnect = lambda: disconnected.append(True)
    with pytest.raises(ResponseError, match="no permissions"):
        list(conn.read_response_lines_streaming())
    assert disconnected == []


def test_read_response_lines_streaming_timeout_midstream_disconnects():
    from redis._parsers.resp2 import _RESP2Parser

    reply, _, _ = make_reply(5)
    conn = Connection()
    parser = _RESP2Parser(socket_read_size=4)
    parser._buffer = SocketBuffer(
        TimeoutInjectingSocket(chunk_bytes(reply, 4)[:2]),  # cut off mid-payload
        socket_read_size=4,
        socket_timeout=1,
    )
    conn._parser = parser
    disconnected = []
    conn.disconnect = lambda: disconnected.append(True)
    with pytest.raises(RedisTimeoutError):
        list(conn.read_response_lines_streaming())
    assert disconnected == [True]


def test_read_response_lines_streaming_skips_resp3_push_frame():
    push = b">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfoo\r\n"
    reply, _, _ = make_reply(2)
    conn, parser = _make_resp3_connection(chunk_bytes(push + reply, 3))
    seen = []
    parser.set_invalidation_push_handler(lambda msg: seen.append(msg))

    lines = list(conn.read_response_lines_streaming())
    assert len(lines) == 2
    assert seen == [["invalidate", ["foo"]]]


def test_read_response_lines_streaming_handles_resp3_verbatim_string():
    # a real redis-server replies to CLIENT LIST under RESP3 with a
    # verbatim string ("=..."), not a plain bulk string ("$...") - the
    # 4-byte type tag ("txt:") must be discarded, not treated as content.
    lines_content = b"id=0 addr=x:1\nid=1 addr=x:2\n"
    payload = b"txt:" + lines_content
    reply = b"=%d\r\n%s\r\n" % (len(payload), payload)
    conn, _parser = _make_resp3_connection(chunk_bytes(reply, 3))

    lines = list(conn.read_response_lines_streaming())
    assert lines == [b"id=0 addr=x:1", b"id=1 addr=x:2"]

    # every byte of the reply must be consumed exactly - no leftover,
    # no over-read into whatever would come next on the wire
    assert conn._parser._buffer.unread_bytes() == 0


def test_read_bulk_lines_rejects_oversized_unterminated_line():
    huge_line = b"x" * 5000  # no embedded '\n'
    payload = huge_line + b"\n"
    buffer = SocketBuffer(FakeSocket([]), socket_read_size=512, socket_timeout=1)
    buffer.MAX_UNTERMINATED_LINE_SIZE = 1024
    buffer._sock = FakeSocket(chunk_bytes(payload, 512))
    with pytest.raises(ConnectionError, match="exceeded"):
        list(buffer.read_bulk_lines(len(payload)))


def test_read_bulk_lines_rejects_negative_length():
    buffer = SocketBuffer(FakeSocket([]), socket_read_size=64, socket_timeout=1)
    with pytest.raises(ConnectionError):
        list(buffer.read_bulk_lines(-2))


def test_read_bulk_lines_rejects_oversized_line_terminated_in_same_chunk():
    # a round-17 review found that the size guard used to only run in
    # the branch taken when `pending` had no '\n' yet (an `elif` sibling
    # of the split-and-yield branch) - so a record whose OWN terminating
    # '\n' arrives in the same recv() chunk as its (oversized) content
    # never spent a loop iteration sitting unterminated at that size,
    # and went straight from "short, no terminator" to "has a
    # terminator, splits into a complete oversized line" without the
    # guard ever seeing it. Reproduced directly: with the guard as an
    # `elif`, this exact payload was returned successfully with no
    # ConnectionError, even though the line exceeds
    # MAX_UNTERMINATED_LINE_SIZE.
    huge_line = b"x" * 101
    payload = huge_line + b"\n"
    buffer = SocketBuffer(FakeSocket([]), socket_read_size=1000, socket_timeout=1)
    buffer.MAX_UNTERMINATED_LINE_SIZE = 100
    buffer._sock = FakeSocket([payload + b"\r\n"])
    with pytest.raises(ConnectionError, match="exceeded"):
        list(buffer.read_bulk_lines(len(payload)))


def test_read_bulk_lines_rejects_oversized_tail_left_by_a_split():
    # the sibling gap to the one above: the TAIL left behind by a split
    # (rather than a completed line) can itself already be oversized,
    # and if `remaining` reaches 0 on that same iteration, the loop used
    # to `break` immediately afterward with no further trip through the
    # (then-`elif`) size check at all - the tail was yielded via the
    # post-loop `if pending: yield ...` completely unchecked. Reproduced
    # directly: this payload (a short terminated line followed by an
    # oversized unterminated tail, all in one chunk) used to be returned
    # successfully with no ConnectionError.
    payload = b"short line\n" + b"A" * 200
    buffer = SocketBuffer(FakeSocket([]), socket_read_size=1000, socket_timeout=1)
    buffer.MAX_UNTERMINATED_LINE_SIZE = 100
    buffer._sock = FakeSocket([payload + b"\r\n"])
    with pytest.raises(ConnectionError, match="exceeded"):
        list(buffer.read_bulk_lines(len(payload)))


def test_read_bulk_lines_accepts_crlf_terminated_line_exactly_at_the_boundary():
    # a round-23 review found an off-by-one: both size checks compared
    # the RAW line/tail bytes - which, for a CRLF-terminated record,
    # still include the trailing '\r' at that point - against
    # MAX_UNTERMINATED_LINE_SIZE, before the '\r'-stripping that only
    # happens afterward, at yield time. So a record whose real content
    # is exactly MAX_UNTERMINATED_LINE_SIZE bytes, terminated with
    # '\r\n', was rejected as "exceeded" even though it had not actually
    # exceeded the documented limit - the effective cap for such a
    # record was MAX-1, not MAX. Reproduced directly against the
    # unfixed code: `b"x"*20 + b"\r\n"` with MAX=20 raised ConnectionError
    # even though the real content is exactly 20 bytes.
    content = b"x" * 20
    payload = content + b"\r\n"
    buffer = SocketBuffer(FakeSocket([]), socket_read_size=1000, socket_timeout=1)
    buffer.MAX_UNTERMINATED_LINE_SIZE = 20
    buffer._sock = FakeSocket([payload + b"\r\n"])
    assert list(buffer.read_bulk_lines(len(payload))) == [content]


def test_read_bulk_lines_still_rejects_content_one_byte_over_the_boundary():
    # the flip side of the test above, bracketing the exact boundary:
    # content genuinely one byte OVER MAX_UNTERMINATED_LINE_SIZE must
    # still be rejected, CRLF-terminated or not - the fix must not
    # widen the effective cap beyond the documented limit, only stop
    # narrowing it by one for CRLF-terminated records.
    content = b"x" * 21
    payload = content + b"\r\n"
    buffer = SocketBuffer(FakeSocket([]), socket_read_size=1000, socket_timeout=1)
    buffer.MAX_UNTERMINATED_LINE_SIZE = 20
    buffer._sock = FakeSocket([payload + b"\r\n"])
    with pytest.raises(ConnectionError, match="exceeded"):
        list(buffer.read_bulk_lines(len(payload)))


def test_parse_client_list_line_handles_embedded_equals_and_nonascii():
    # values may contain '=' (e.g. base64-ish lib-ver strings) and non-ASCII
    # bytes (e.g. a CLIENT SETNAME with UTF-8 content); neither may contain
    # a literal space, which is the actual wire-format constraint here.
    line = "id=1 addr=127.0.0.1:1 name=café lib-ver=a=b".encode()
    parsed = parse_client_list_line(line)
    assert parsed["id"] == "1"
    assert parsed["name"] == "café"
    assert parsed["lib-ver"] == "a=b"


def test_parse_client_list_splits_only_on_literal_newline():
    # a round-17 review found this fix had no regression test at all:
    # reverting parse_client_list() back to str.splitlines() (which also
    # treats '\v', '\f', '\x1c'-'\x1e', NEL, U+2028/2029 etc. as line
    # boundaries, not just '\n') passed the entire suite unchanged.
    # A field value containing one of those control characters (e.g. a
    # CLIENT SETNAME) must stay part of the SAME record under
    # parse_client_list() - matching client_list_iter()'s own strict
    # '\n'-only splitting (SocketBuffer.read_bulk_lines) - rather than
    # being wrongly split into two records.
    from redis._parsers.helpers import parse_client_list

    response = "id=1 addr=x:1 name=ab\x0bcd\nid=2 addr=x:2 name=ef\n"
    parsed = parse_client_list(response)
    assert len(parsed) == 2
    assert parsed[0]["name"] == "ab\x0bcd"
    assert parsed[1]["id"] == "2"


def test_client_list_args_encodes_bytes_client_ids_correctly():
    # client_id is typed to accept bytes/memoryview elements (EncodableT),
    # not just str/int - str(b"123") gives "b'123'" (Python's repr), which
    # would send corrupted data to the server instead of the id's actual
    # decoded text.
    #
    # Each id is its own separate argument (not one joined string) - a
    # round-25 review found the previous, joined-string form was a real,
    # live-verified bug: redis-server rejects "CLIENT LIST ID <a> <b>"
    # sent as ONE argument ("<a> <b>") with "Invalid client ID", only
    # accepting multiple ids as separate arguments.
    from redis.commands.core import _client_list_args

    args = _client_list_args(None, [b"123", memoryview(b"456"), "789", 42])
    assert args == [b"ID", "123", "456", "789", "42"]


def test_client_list_args_encodes_bytearray_and_whole_number_float_ids():
    from redis.commands.core import _client_list_args

    # bytearray hits the exact same str()-mangling risk as bytes/memoryview
    args = _client_list_args(None, [bytearray(b"123")])
    assert args == [b"ID", "123"]

    # str(42.0) == "42.0", which the server rejects as an invalid id;
    # a whole-number float should format the same as the equivalent int
    args = _client_list_args(None, [42.0])
    assert args == [b"ID", "42"]


def test_client_list_args_rejects_non_utf8_bytes_client_id_cleanly():
    from redis.commands.core import _client_list_args
    from redis.exceptions import DataError

    with pytest.raises(DataError):
        _client_list_args(None, [b"\xff\xfe"])


def test_client_list_args_rejects_combining_type_and_client_id():
    # a round-26 review found redis-server itself rejects CLIENT LIST
    # TYPE ... ID ... with a plain "syntax error" - verified live
    # (redis-cli client list type normal id 1 -> "ERR syntax error").
    # Raising a clear DataError here instead of letting that cryptic
    # error surface from the server.
    from redis.commands.core import _client_list_args
    from redis.exceptions import DataError

    with pytest.raises(DataError):
        _client_list_args("normal", ["1"])


def test_client_list_iter_matches_client_list_decoding_for_custom_encoding():
    # decode_responses=True: client_list() already gets the response
    # pre-decoded by the connection's real Encoder before parse_client_list()
    # ever runs, so it honors a non-default encoding - client_list_iter()
    # must replicate that, not fall back to hardcoded utf-8.
    #
    # This exercises _client_list_iter_gen() itself (the actual code that
    # computes `encoding`/`encoding_errors` from conn.encoder), not just
    # parse_client_list_line() called manually with those values already
    # supplied - a prior version of this test did the latter and, as a
    # result, did not notice a real regression where _client_list_iter_gen()
    # stopped consulting conn.encoder at all and silently hardcoded
    # utf-8/"replace" unconditionally.
    from redis._parsers.encoders import Encoder
    from redis._parsers.helpers import parse_client_list
    from redis.commands.core import _client_list_iter_gen

    raw_line = b"id=1 addr=127.0.0.1:1 name=caf\xe9"  # valid latin-1, invalid utf-8
    encoder = Encoder(
        encoding="latin-1", encoding_errors="strict", decode_responses=True
    )

    via_client_list = parse_client_list(encoder.decode(raw_line + b"\n"))

    conn = _FakeConnection([raw_line])
    conn.encoder = encoder
    via_client_list_iter = list(_client_list_iter_gen(conn, []))

    expected = [{"id": "1", "addr": "127.0.0.1:1", "name": "café"}]
    assert via_client_list == via_client_list_iter == expected


def test_client_list_iter_matches_client_list_decoding_when_decode_responses_false():
    # decode_responses=False: parse_client_list()'s own str_if_bytes() call
    # force-decodes with hardcoded utf-8/"replace" regardless of the
    # configured encoding (a pre-existing quirk) - client_list_iter() must
    # match that quirk too, not "improve" on it and diverge.
    from redis._parsers.helpers import parse_client_list

    raw_line = b"id=1 addr=127.0.0.1:1 name=caf\xe9"

    via_client_list = parse_client_list(raw_line + b"\n")
    via_client_list_iter = parse_client_list_line(raw_line)  # defaults
    assert via_client_list == [via_client_list_iter]
    assert via_client_list[0]["name"] == "caf�"  # utf-8 replacement char


class _FakeConnection:
    """Minimal stand-in for redis.connection.Connection used by client_list_iter."""

    def __init__(self, lines, streamable=True):
        self._lines = lines
        self._streamable = streamable
        self.sent = []
        self.disconnected = False
        self.encoder = type(
            "_FakeEncoder",
            (),
            {
                "encoding": "utf-8",
                "encoding_errors": "replace",
                "decode_responses": False,
            },
        )()

    def can_stream_lines(self):
        return self._streamable

    def send_command(self, *args):
        self.sent.append(args)

    def read_response_lines_streaming(self):
        # mirrors the real Connection.read_response_lines_streaming():
        # abandonment (GeneratorExit) disconnects before propagating.
        try:
            yield from self._lines
        except GeneratorExit:
            self.disconnect()
            raise

    def disconnect(self):
        self.disconnected = True


class _FakePool:
    """
    Faithfully models enough of ConnectionPool's real available/in-use
    bookkeeping and disconnect(inuse_connections=...) semantics (see
    redis/connection.py) to actually exercise Redis.close()'s
    still_in_use handling - a prior version of this stub's disconnect()
    was an unconditional no-op, which let a real bug (auto_close_
    connection_pool's cleanup disconnecting a connection Redis.close()
    had just decided to leave alone) hide behind a passing test.
    """

    def __init__(self, conn):
        self._conn = conn
        self.available = []
        self._in_use = {conn}

    def get_connection(self, *args, **kwargs):
        self._in_use.add(self._conn)
        return self._conn

    def release(self, conn):
        self._in_use.discard(conn)
        self.available.append(conn)

    def disconnect(self, inuse_connections=True):
        targets = list(self.available)
        if inuse_connections:
            targets += list(self._in_use)
        for conn in targets:
            conn.disconnect()

    def close(self):
        # matches the real ConnectionPool.close(), which is just a thin
        # wrapper: "def close(self): self.disconnect()" - Redis.close()
        # calls this (not disconnect() directly) for its plain,
        # nothing-still-in-use case.
        self.disconnect()


class _SlowGetConnectionPool(_FakePool):
    """Like _FakePool, but get_connection() genuinely blocks for a while -
    simulating an exhausted BlockingConnectionPool or a slow TCP/TLS
    connect - so a racing close() call on an UNRELATED client can be
    checked for whether it gets stuck waiting on the same resource."""

    def __init__(self, conn, delay):
        super().__init__(conn)
        self.delay = delay
        self.get_connection_started = threading.Event()

    def get_connection(self, *args, **kwargs):
        self.get_connection_started.set()
        time.sleep(self.delay)
        return super().get_connection(*args, **kwargs)


def test_client_list_iter_close_blocks_for_a_concurrent_in_flight_registration():
    # single_connection_client mode: self.connection is already an
    # established connection, so nothing in client_list_iter() has to
    # block on I/O before registering. This test verifies that a
    # concurrent close() call genuinely BLOCKS (waiting on the shared
    # lock) rather than racing past a registration that is still
    # in-flight (widened here via a slow can_stream_lines() stand-in) -
    # otherwise close() could see nothing registered yet and release
    # the connection out from under the in-flight call, letting two
    # callers end up driving the same live connection at once.
    #
    # A round-22 review correctly found that this test's ORIGINAL
    # docstring overclaimed: it does NOT verify that the READ of self.
    # connection specifically happens inside the lock (only that
    # whatever runs after client_list_iter() has already acquired the
    # lock - the slow can_stream_lines() stand-in - stays atomic).
    # Reverting that specific fix (moving the read of self.connection
    # to just BEFORE the lock instead of as the first statement inside
    # it) still passes this test unchanged, confirmed directly. That
    # more specific property - the read itself, not just what follows
    # it, happening under the lock - is what test_client_list_iter_
    # registers_while_holding_the_lock verifies instead, via a
    # structural check on the read itself rather than timing.
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1"])
    # widen the (normally near-instantaneous) window between
    # client_list_iter() acquiring the lock and completing registration,
    # the same way a real adversarial timing repro would, to make the
    # race deterministically observable in a fast unit test.
    registration_gate = threading.Event()

    def slow_can_stream_lines():
        registration_gate.wait(timeout=5)
        return True

    conn.can_stream_lines = slow_can_stream_lines
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn

    def do_client_list_iter():
        list(r.client_list_iter())

    t = threading.Thread(target=do_client_list_iter)
    t.start()
    time.sleep(0.05)  # let it block inside can_stream_lines(), pre-registration

    close_done = threading.Event()

    def do_close():
        r.close()
        close_done.set()

    closer = threading.Thread(target=do_close)
    closer.start()
    # close() must be BLOCKED waiting for the same lock right now - it
    # must not have raced ahead and released/disconnected conn while
    # registration was still pending.
    time.sleep(0.05)
    assert not close_done.is_set(), (
        "close() completed before the in-flight client_list_iter() call "
        "finished registering - it raced past the lock instead of "
        "waiting, reopening the exact corruption risk this test guards "
        "against"
    )
    assert conn.disconnected is False

    registration_gate.set()  # let registration (and the read) proceed
    t.join()
    closer.join()


def test_client_list_iter_owners_lock_does_not_block_unrelated_close():
    # _client_list_iter_owners_lock is a single process-wide lock shared
    # by every Redis client's client_list_iter()/close() calls - it must
    # never be held across pool.get_connection() (which can block for
    # real), or one client's slow/exhausted pool would stall close() for
    # every OTHER, completely unrelated client in the whole process.
    import redis

    slow_conn = _FakeConnection([b"id=0 addr=x:1"])
    slow_pool = _SlowGetConnectionPool(slow_conn, delay=1.0)
    slow_client = redis.Redis.from_url("redis://localhost:6379/0")
    slow_client.connection_pool = slow_pool
    # pooled mode: self.connection stays None, forcing get_connection()

    def slow_call():
        list(slow_client.client_list_iter())

    t = threading.Thread(target=slow_call)
    t.start()
    slow_pool.get_connection_started.wait(timeout=5)  # it's now blocked

    other_conn = _FakeConnection([b"id=0 addr=x:1"])
    other_pool = _FakePool(other_conn)
    other_client = redis.Redis.from_url("redis://localhost:6379/0")
    other_client.connection_pool = other_pool

    start = time.monotonic()
    other_client.close()
    elapsed = time.monotonic() - start

    t.join()
    assert elapsed < 0.5, (
        f"close() on an unrelated client took {elapsed:.2f}s - it was "
        "blocked by another client's slow pool.get_connection() call"
    )


class _SlowDisconnectPool(_FakePool):
    """Like _FakePool, but disconnect() genuinely blocks for a while -
    simulating a large pool or a slow/unresponsive peer during socket
    teardown - so a racing close() call on an UNRELATED client can be
    checked for whether it gets stuck waiting on the same global lock."""

    def __init__(self, conn, delay):
        super().__init__(conn)
        self.delay = delay

    def disconnect(self, inuse_connections=True):
        time.sleep(self.delay)
        return super().disconnect(inuse_connections=inuse_connections)


def test_redis_close_does_not_hold_lock_across_disconnect():
    # a round-18 review found that an earlier version of close()'s final
    # auto_close_connection_pool block held _client_list_iter_owners_lock
    # (the SAME process-wide lock test_client_list_iter_owners_lock_does_
    # not_block_unrelated_close above guards for pool.get_connection())
    # across connection_pool.disconnect() itself too - so a slow
    # disconnect() on ONE client's pool (e.g. many connections to tear
    # down, or a slow/unresponsive peer) stalled close() for every OTHER,
    # completely unrelated client in the process, even one that never
    # touched client_list_iter() at all. Reproduced directly against the
    # real Redis.close() code before this was fixed.
    import redis

    slow_conn = _FakeConnection([b"id=0 addr=x:1"])
    slow_pool = _SlowDisconnectPool(slow_conn, delay=1.0)
    slow_client = redis.Redis.from_url("redis://localhost:6379/0")
    slow_client.connection_pool = slow_pool

    def slow_close():
        slow_client.close()

    t = threading.Thread(target=slow_close)
    t.start()
    time.sleep(0.1)  # let slow_client's close() get into its slow disconnect()

    other_conn = _FakeConnection([b"id=0 addr=x:1"])
    other_pool = _FakePool(other_conn)
    other_client = redis.Redis.from_url("redis://localhost:6379/0")
    other_client.connection_pool = other_pool

    start = time.monotonic()
    other_client.close()
    elapsed = time.monotonic() - start

    t.join()
    assert elapsed < 0.5, (
        f"close() on an unrelated client took {elapsed:.2f}s - it was "
        "blocked by another client's slow connection_pool.disconnect() call"
    )


def test_client_list_iter_releases_after_plain_full_iteration():
    # the common case: no `with`, no explicit close() - just draining it
    # via a plain for-loop/list() call, same as scan_iter() usage.
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    rows = list(r.client_list_iter())
    assert [row["id"] for row in rows] == ["0", "1"]
    assert pool.available == [conn]


def test_client_list_iter_owners_lock_is_reentrant():
    # while _client_list_iter_owners_lock is held (by any thread's
    # client_list_iter()/close() call), an ordinary allocation can
    # trigger CPython's cyclic GC, which (since PEP 442) runs __del__
    # SYNCHRONOUSLY on that same thread for any unrelated cycle-collected
    # object it finds. If that object's __del__ happens to be some other
    # Redis instance's close() (also taking this same lock), a
    # non-reentrant lock would deadlock the thread against itself - so
    # the lock must be reentrant (a threading.RLock).
    #
    # This deliberately does NOT test that via gc.collect() triggering a
    # real __del__: a prior version of this test did exactly that, and it
    # turned out to be a false-negative regression test - reverting the
    # lock to a plain, non-reentrant threading.Lock made the reentrant
    # acquire inside __del__ hang, but pytest-timeout's signal-based
    # watchdog then raised INTO that __del__ frame after its full
    # timeout, and Python's unraisable-exception handling silently
    # swallows an exception raised inside __del__ - so the test still
    # reported as PASSED (after burning the full timeout), completely
    # defeating its purpose. Testing the SAME-THREAD reentrancy property
    # directly, from a dedicated background thread bounded by a short
    # join() timeout, fails fast and cleanly instead.
    from redis.commands.core import _client_list_iter_owners_lock

    succeeded = threading.Event()

    def acquire_twice_on_same_thread():
        with _client_list_iter_owners_lock:
            with _client_list_iter_owners_lock:
                succeeded.set()

    t = threading.Thread(target=acquire_twice_on_same_thread, daemon=True)
    t.start()
    t.join(timeout=2)
    assert succeeded.is_set(), (
        "re-acquiring _client_list_iter_owners_lock on the same thread "
        "that already holds it did not complete within 2s - the lock is "
        "no longer reentrant (a plain threading.Lock would deadlock "
        "here; it must stay a threading.RLock)"
    )


def test_client_list_iter_owners_registration_is_thread_safe():
    # concurrent client_list_iter() calls (each doing owners.add(it))
    # racing a concurrent close() (doing list(owners)) must never raise
    # "RuntimeError: Set changed size during iteration" out of close(),
    # and must never silently lose an iterator's registration to a
    # lost-update race on lazily creating the WeakSet itself.
    #
    # Note: the underlying race this guards against is inherently rare
    # even without the fix (reported as ~10 crashes out of ~6M
    # create/close iterations under heavy stress) - this test's job is
    # a "no crash under concurrent load" smoke check, not a guaranteed
    # pre-fix failure demonstration; the fix itself (a lock around both
    # operations) eliminates the race by construction regardless of
    # whether this specific run happens to hit the exact window.
    import redis

    errors = []

    class _FreshConnectionPerCallPool:
        """Unlike _FakePool (which deliberately always returns the SAME
        tracked connection object, so other tests can assert on its
        specific state), this hands out a genuinely fresh, independent
        connection per get_connection() call - matching how a real
        ConnectionPool actually behaves: it never hands the same live
        connection to two concurrent callers. Needed here specifically:
        this test intentionally races many concurrent client_list_iter()
        calls, and _FakePool's single shared connection - a plain,
        non-thread-safe test double, never meant to be hit from several
        threads at once - was found (via a CI segfault under coverage.py's
        thread-tracing) to crash for that reason alone, unrelated to what
        this test actually intends to exercise (the _client_list_iter_
        owners WeakSet's own registration/snapshot thread-safety)."""

        def get_connection(self, *args, **kwargs):
            return _FakeConnection([b"id=0 addr=x:1"])

        def release(self, conn):
            pass

        def disconnect(self, inuse_connections=True):
            pass

        def close(self):
            pass

    def make_client():
        pool = _FreshConnectionPerCallPool()
        r = redis.Redis.from_url("redis://localhost:6379/0")
        r.connection_pool = pool
        return r

    r = make_client()
    stop = threading.Event()

    def creator():
        while not stop.is_set():
            try:
                it = r.client_list_iter()
                next(it, None)
                it.close()
            except Exception as e:  # pragma: no cover - failure path
                errors.append(e)

    def closer():
        while not stop.is_set():
            try:
                r.close()
            except Exception as e:  # pragma: no cover - failure path
                errors.append(e)

    threads = [threading.Thread(target=creator) for _ in range(6)]
    threads.append(threading.Thread(target=closer))
    for t in threads:
        t.start()
    time.sleep(0.5)
    stop.set()
    for t in threads:
        t.join()

    assert errors == []


def test_client_list_iter_hiredis_guard_fires_before_send():
    import redis

    conn = _FakeConnection([], streamable=False)
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    with pytest.raises(NotImplementedError):
        list(r.client_list_iter())
    assert conn.sent == []  # CLIENT LIST must never reach the wire
    assert pool.available == [conn]  # checked-out connection must be released


def test_client_list_iter_hiredis_guard_fires_for_single_connection_client_too():
    # a round-23 review found the pooled-mode hiredis guard above had a
    # direct test, but the single_connection_client-mode branch (reached
    # while holding _client_list_iter_owners_lock, self.connection
    # already established) had no direct unit test of its own.
    import redis

    conn = _FakeConnection([], streamable=False)
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn  # single_connection_client mode
    with pytest.raises(NotImplementedError):
        list(r.client_list_iter())
    assert conn.sent == []  # CLIENT LIST must never reach the wire
    # single_connection_client mode never checks anything out of/into
    # the pool at all - self.connection is untouched either way.
    assert r.connection is conn
    assert pool.available == []


class _ApplicationErrorFakeConnection(_FakeConnection):
    """Simulates an ordinary application-level error reply (e.g. NOPERM)
    at the point read_response_lines_streaming() would normally start
    yielding lines - the real Connection.read_response_lines_streaming()
    raises such errors without disconnecting (see
    test_read_response_lines_streaming_ordinary_error_does_not_
    disconnect, which exercises that lower-level behavior directly);
    this exercises the SAME behavior through the full client_list_iter()
    / _ClientListIter stack instead."""

    def read_response_lines_streaming(self):
        raise ResponseError("NOPERM this user has no permissions")
        yield  # pragma: no cover - makes this a generator function


def test_client_list_iter_releases_connection_after_application_error():
    # a round-23 review found that only the lower-level Connection.
    # read_response_lines_streaming() had a test for an ordinary
    # application-level error reply (e.g. NOPERM) not disconnecting -
    # nothing exercised the SAME scenario through the full client_list_
    # iter()/_ClientListIter stack, confirming the connection still gets
    # released back to the pool (not leaked, not disconnected) when the
    # very first thing read back is an application error.
    import redis

    conn = _ApplicationErrorFakeConnection([])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    with pytest.raises(ResponseError):
        list(r.client_list_iter())

    assert conn.disconnected is False
    assert pool.available == [conn]


def test_client_list_iter_does_not_double_release():
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn  # simulate single_connection_client mode

    gen = r.client_list_iter()
    next(gen)  # start iterating; conn captured as "not from pool" (self.connection set)
    r.connection = None  # simulate close() clearing self.connection mid-stream
    gen.close()  # unwinds the generator's finally

    # from_pool was captured as False at acquire time, so closing the
    # generator after self.connection was cleared must NOT release conn
    # into the pool a second time.
    assert pool.available == []


def test_redis_close_disconnects_connection_still_streaming_client_list():
    # single_connection_client=True mode: self.connection IS the exact
    # connection a live client_list_iter() generator is reading from.
    # Redis.close() releasing it into the shared pool while still
    # mid-stream would hand an actively-in-use socket to an unrelated
    # caller of that pool - close() must disconnect it first instead.
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn  # simulate single_connection_client=True

    it = r.client_list_iter()
    next(it)
    assert it in r._client_list_iter_owners  # registered on the client
    assert conn.disconnected is False

    r.close()  # the real close(), not a manual simulation

    assert conn.disconnected is True  # disconnected before being released
    assert pool.available == [
        conn
    ]  # released exactly once, to a dead (safe) connection


def test_redis_close_does_not_disconnect_when_not_streaming():
    # close() must NOT unconditionally disconnect every connection it
    # releases - only ones flagged as still actively streaming - so
    # ordinary close() behavior for unrelated connections is unaffected.
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn
    # isolate the owner-delegation logic under test from the separate,
    # pre-existing auto_close_connection_pool mechanism (also on by
    # default from from_url()), which would otherwise disconnect this
    # same connection anyway and make this assertion pass for the wrong
    # reason regardless of whether the logic under test is correct.
    r.auto_close_connection_pool = False

    r.close()

    assert conn.disconnected is False
    assert pool.available == [conn]


def test_client_list_iter_validates_before_returning_generator():
    import redis

    r = redis.Redis.from_url("redis://localhost:6379/0")
    with pytest.raises(redis.DataError):
        r.client_list_iter(_type="bogus")  # must raise here, not on first next()


def test_client_list_iter_rejects_pipeline():
    import redis

    r = redis.Redis.from_url("redis://localhost:6379/0")
    pipe = r.pipeline()
    with pytest.raises(NotImplementedError):
        pipe.client_list_iter()
    pipe.reset()


def test_client_list_iter_disconnects_on_early_abandonment():
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    gen = r.client_list_iter()
    next(gen)  # only consume the first of two lines
    assert conn.disconnected is False
    gen.close()  # abandon mid-stream: must disconnect, then release once
    assert conn.disconnected is True
    assert pool.available == [conn]


def test_client_list_iter_releases_even_if_never_iterated():
    # a bare generator's try/finally body never runs if next() is never
    # called even once - client_list_iter() must not depend on that for
    # releasing the connection it eagerly checked out.
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    it = r.client_list_iter()
    assert conn.sent == []  # nothing sent yet, matching lazy send_command
    assert pool.available == []  # still checked out
    it.close()  # never called next() on it at all
    assert pool.available == [conn]


def test_client_list_iter_releases_via_garbage_collection():
    # dropping the iterator without close()/with/full-iteration must still
    # release the connection, via __del__, once nothing references it.
    import gc

    import redis

    conn = _FakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    it = r.client_list_iter()
    next(it)
    assert pool.available == []

    del it
    gc.collect()
    assert pool.available == [conn]


def test_client_list_iter_survives_dropping_the_owning_client():
    # a round-21 review found that the returned iterator held no
    # reference back to the Redis client that created it - only the
    # client held a WEAK reference to the iterator (deliberately, so an
    # abandoned iterator doesn't keep an otherwise-unused client alive
    # forever). Since Redis.close() runs unconditionally from
    # Redis.__del__, this meant an entirely ordinary-looking one-liner
    # like `rows = list(redis.Redis(...).client_list_iter())` silently
    # returned an EMPTY list with no exception: the temporary Redis
    # object's refcount hit zero the instant client_list_iter() returned
    # (before the caller ever called next()), triggering __del__ ->
    # close() -> closing and releasing this very iterator, having never
    # been started, all before list() could consume anything from it.
    # Reproduced directly against the actual (unfixed) code in both
    # pooled and single_connection_client modes. Fixed by having the
    # iterator hold its own STRONG reference back to the client, keeping
    # it alive for exactly as long as the iterator itself is referenced
    # - not a cycle, since the client's own reference to the iterator
    # stays weak.
    import gc

    import redis

    def make_pooled_iter():
        conn = _FakeConnection([b"id=0 addr=x:1"])
        pool = _FakePool(conn)
        r = redis.Redis.from_url("redis://localhost:6379/0")
        r.connection_pool = pool
        return r.client_list_iter()

    it = make_pooled_iter()
    gc.collect()  # the temporary client's refcount already hit zero
    # without needing this at all - collecting anyway just rules out any
    # "well it just hasn't been GC'd yet" objection.
    assert list(it) == [{"id": "0", "addr": "x:1"}]

    def make_single_connection_client_iter():
        conn = _FakeConnection([b"id=0 addr=x:1"])
        pool = _FakePool(conn)
        r = redis.Redis.from_url("redis://localhost:6379/0")
        r.connection_pool = pool
        r.connection = conn
        return r.client_list_iter()

    it2 = make_single_connection_client_iter()
    gc.collect()
    assert list(it2) == [{"id": "0", "addr": "x:1"}]


def test_client_list_iter_supports_with_block():
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    with r.client_list_iter() as it:
        rows = list(it)
    assert [row["id"] for row in rows] == ["0", "1"]
    assert pool.available == [conn]  # released on __exit__


def test_client_list_iter_with_block_releases_on_exception():
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    with pytest.raises(ValueError):
        with r.client_list_iter() as it:
            next(it)
            raise ValueError("boom")
    assert pool.available == [conn]  # released even though body raised


def test_client_list_iter_disconnects_real_connection_on_abandonment():
    # exercises the real Connection.read_response_lines_streaming() +
    # SocketBuffer.read_bulk_lines() chain (not the hand-written
    # _FakeConnection stub) through an actual mid-stream abandonment, via
    # the public client_list_iter() API.
    import redis

    reply, _, _ = make_reply(5)
    conn = _make_resp2_connection(chunk_bytes(reply, 4))
    disconnected = []
    conn.disconnect = lambda: disconnected.append(True)
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    it = r.client_list_iter()
    next(it)
    assert disconnected == []
    it.close()
    assert disconnected == [True]
    assert pool.available == [conn]


class _SlowFakeConnection(_FakeConnection):
    """Like _FakeConnection, but genuinely blocks between lines so another
    thread's close() call races against an in-flight next(). Sets
    reading_started (rather than making callers guess with a fixed
    time.sleep) as soon as it has actually entered the blocking read, so
    a racing thread can deterministically wait for that instead of
    hoping a fixed delay was long enough under any given scheduler load."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reading_started = threading.Event()

    def read_response_lines_streaming(self):
        try:
            for line in self._lines:
                self.reading_started.set()
                time.sleep(0.2)
                yield line
        except GeneratorExit:
            self.disconnect()
            raise


def test_client_list_iter_concurrent_close_does_not_release_in_flight_conn():
    import redis

    conn = _SlowFakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    it = r.client_list_iter()

    def consume():
        list(it)

    t = threading.Thread(target=consume)
    t.start()
    conn.reading_started.wait(timeout=5)  # deterministic, not a fixed sleep guess

    # closing from the main thread while the consumer thread is genuinely
    # still executing next() must NOT release the still-in-use connection
    with pytest.raises(RuntimeError, match="another thread"):
        it.close()
    assert pool.available == []

    t.join()
    # once the consumer thread has actually finished, the connection must
    # eventually be released (via __next__'s own close() on StopIteration)
    assert pool.available == [conn]


def test_redis_close_leaks_rather_than_releases_a_genuinely_in_flight_conn():
    # single_connection_client=True mode, with another thread genuinely
    # still reading: Redis.close() must NOT release conn into the pool
    # (that would hand out a still-live socket) - it may leak it from
    # the pool's perspective instead, which is the safe trade-off.
    import redis

    conn = _SlowFakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    # a second, unrelated, already-idle connection in the same pool -
    # proves the fix's actual mechanism (inuse_connections=False skips
    # ALL currently-in-use connections as a category, not this one
    # connection specifically): auto_close_connection_pool's cleanup must
    # still disconnect this idle one, while sparing the busy one.
    idle_conn = _FakeConnection([])
    pool.available.append(idle_conn)
    pool._in_use.discard(idle_conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn  # simulate single_connection_client=True

    it = r.client_list_iter()

    def consume():
        list(it)

    t = threading.Thread(target=consume)
    t.start()
    conn.reading_started.wait(timeout=5)  # deterministic, not a fixed sleep guess

    r.close()  # races the still-running generator in the other thread
    # conn itself must NOT have been released (only idle_conn, which was
    # already idle before this call, is present)
    assert pool.available == [idle_conn]
    assert idle_conn.disconnected is True  # still-idle connections do get swept
    assert conn.disconnected is False  # NOT released while still genuinely in use

    t.join()
    # the other thread went on to successfully finish draining the reply
    # (Redis.close() only gave up on releasing/disconnecting it, it did
    # not - and could not - stop that thread's already in-flight read).
    # A successful drain never disconnects on its own (only abandonment
    # or an error does), and single_connection_client mode never routes
    # pool.release() through the iterator itself (from_pool=False) - so
    # nobody hands this connection back to the pool in this scenario. It
    # ends up orphaned from the pool's bookkeeping but perfectly healthy
    # and never corrupted - the documented, accepted "leak rather than
    # corrupt" trade-off for this narrow, already-unsupported combination
    # of concurrent misuses (sharing a single_connection_client across
    # threads while also concurrently closing it).
    assert pool.available == [idle_conn]
    assert conn.disconnected is False  # never touched, not corrupted either
    assert idle_conn.disconnected is True  # but other idle connections still are


class _PlainDisconnectPool(_FakePool):
    """Like _FakePool, but its disconnect() takes no inuse_connections
    parameter at all - mirroring BlockingConnectionPool's real signature
    (redis/connection.py), which close() must remain compatible with."""

    def disconnect(self):
        for conn in list(self.available) + list(self._in_use):
            conn.disconnect()


def test_redis_close_compatible_with_pool_lacking_inuse_connections_param():
    # a real regression this round: passing inuse_connections= to every
    # connection_pool.disconnect() call broke close() for any pool type
    # (e.g. the real BlockingConnectionPool) whose disconnect() doesn't
    # accept that parameter - even in the ordinary, no-streaming case.
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1"])
    pool = _PlainDisconnectPool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn

    r.close()  # must not raise TypeError

    assert conn.disconnected is True  # ordinary auto_close_connection_pool sweep


def test_redis_close_still_in_use_falls_back_gracefully_without_inuse_param():
    # the still_in_use=True branch specifically: a pool shaped like
    # BlockingConnectionPool (no inuse_connections parameter at all)
    # combined with a genuinely busy client_list_iter() - close() must
    # catch the resulting TypeError and skip the auto_close_connection_pool
    # sweep entirely, rather than let it escape or fall through to the
    # unconditional (unsafe) disconnect() call.
    import redis

    conn = _SlowFakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _PlainDisconnectPool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn  # simulate single_connection_client=True

    it = r.client_list_iter()

    def consume():
        list(it)

    t = threading.Thread(target=consume)
    t.start()
    conn.reading_started.wait(timeout=5)  # deterministic, not a fixed sleep guess

    r.close()  # must not raise TypeError, and must not disconnect conn
    assert conn.disconnected is False

    t.join()


def test_redis_close_called_twice_does_not_double_release():
    # calling close() twice on the same single_connection_client=True
    # instance must release the connection exactly once, not twice - the
    # second call must see self.connection already nulled and do nothing
    # further.
    #
    # This is deliberately NOT a concurrency/timing test: an earlier
    # version of this test used two threads and a bare threading.Barrier
    # to try to force two close() calls to race inside the actual
    # vulnerable window (reading self.connection and nulling it, both
    # under the same lock) - confirmed, by deliberately removing the
    # production fix and re-running it 50 times, to never actually
    # observe the race, passing identically whether the fix was present
    # or not, since that window is only a couple of bytecode instructions
    # wide. A LATER attempt at forcing genuine overlap via a slow,
    # unrelated fake owner in close()'s early pass was checked directly
    # too: it stalls one thread in a part of close() that runs BEFORE
    # that thread ever reads self.connection at all, so by the time it
    # gets there the other thread's close() call has already completed
    # and nulled self.connection - never producing real overlap in the
    # window that matters either, just exercising the (trivially
    # correct) case of calling close() again after self.connection is
    # already None. The actual "is this section atomic" property is
    # covered deterministically and structurally, with no timing
    # involved at all, by test_redis_close_releases_connection_while_
    # holding_the_lock below - this test only checks the plain, simple,
    # non-racy behavior its name describes.
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn

    release_calls = []
    orig_release = pool.release

    def counting_release(c):
        release_calls.append(c)
        return orig_release(c)

    pool.release = counting_release

    r.close()
    assert len(release_calls) == 1
    assert pool.available.count(conn) == 1

    r.close()  # self.connection is already None - must be a no-op
    assert len(release_calls) == 1
    assert pool.available.count(conn) == 1


def test_redis_close_nulls_connection_under_the_lock_before_releasing():
    # a structural, fully deterministic complement to the test above
    # (no threading/timing at all). A round-20 review found that an
    # earlier version of this test asserted the OPPOSITE of what is
    # actually correct: it required connection_pool.release() itself to
    # be called WHILE HOLDING _client_list_iter_owners_lock - but
    # release() can genuinely block for real (ConnectionPool._checkpid()
    # can wait up to 5s on a fork-race lock, or release() calls
    # connection.disconnect(), a real socket-teardown syscall, whenever
    # owns_connection() is False post-fork), and holding this same
    # process-wide lock across it stalls every OTHER, unrelated Redis
    # client's close() call in the process - reproduced directly. The
    # atomicity that actually rules out two concurrent close() calls
    # both releasing the same connection does not require release()
    # itself to be inside the lock - only NULLING self.connection does,
    # since that is the one step that makes a second close() call's own
    # `self.connection is conn` check correctly see None and skip. This
    # test verifies both current properties directly: self.connection is
    # already None by the time release() is called (the actual
    # atomicity guarantee), and the lock is NOT held at that point (the
    # round-20 fix). Timing-based tests of the double-release property
    # itself were separately confirmed unable to detect its removal even
    # with 30 threads racing with zero locking at all - the vulnerable
    # window is only a couple of bytecode instructions wide, far too
    # narrow to reliably hit by chance, which is why this test checks
    # the structural properties directly instead.
    import redis
    import redis.client as client_module
    import redis.commands.core as core_module

    class _TrackingLock:
        """Wraps the real lock, recording whether it's currently held
        (by any thread) at any given moment - swapped in for both
        modules for the duration of this test only."""

        def __init__(self):
            self._real = threading.RLock()
            self.held = False

        def __enter__(self):
            self._real.__enter__()
            self.held = True
            return self

        def __exit__(self, *args):
            self.held = False
            return self._real.__exit__(*args)

    tracking_lock = _TrackingLock()
    orig_core_lock = core_module._client_list_iter_owners_lock
    orig_client_lock = client_module._client_list_iter_owners_lock
    core_module._client_list_iter_owners_lock = tracking_lock
    client_module._client_list_iter_owners_lock = tracking_lock
    try:
        conn = _FakeConnection([b"id=0 addr=x:1"])
        pool = _FakePool(conn)
        r = redis.Redis.from_url("redis://localhost:6379/0")
        r.connection_pool = pool
        r.connection = conn

        observed = []
        orig_release = pool.release

        def checking_release(c):
            observed.append((tracking_lock.held, r.connection))
            return orig_release(c)

        pool.release = checking_release
        r.close()

        assert observed, "connection_pool.release() was never called"
        held_at_release, connection_at_release = observed[0]
        assert held_at_release is False, (
            "connection_pool.release() was called while "
            "_client_list_iter_owners_lock was STILL held - this stalls "
            "every unrelated Redis client's close() call in the process "
            "for however long release() takes"
        )
        assert connection_at_release is None, (
            "self.connection was not yet nulled by the time "
            "connection_pool.release() was called - the step that "
            "actually prevents a double-release did not happen before "
            "release() ran"
        )
    finally:
        core_module._client_list_iter_owners_lock = orig_core_lock
        client_module._client_list_iter_owners_lock = orig_client_lock


def test_client_list_iter_registers_while_holding_the_lock():
    # the client_list_iter() counterpart to the test above: for
    # single_connection_client mode, reading self.connection through
    # registering the resulting iterator must ALL happen while holding
    # _client_list_iter_owners_lock - the property that rules out a
    # concurrent close() call ever running to completion (seeing
    # nothing registered yet) in the gap between this call reading
    # self.connection and registering itself.
    #
    # Checking lock state only at the registration call (WeakSet.add)
    # is NOT sufficient: a regression where the *read* of
    # self.connection happens before the lock is acquired, while
    # registration still happens inside it, would still pass such a
    # check - both events individually see the lock held, even though
    # the read itself (the thing that actually needs protecting) did
    # not. To catch that specific shape of bug, this test intercepts
    # the READ of self.connection itself, via a property on a
    # throwaway subclass, and records lock state at THAT moment - not
    # merely at whatever happens to run afterward.
    import redis
    import redis.client as client_module
    import redis.commands.core as core_module

    class _TrackingLock:
        def __init__(self):
            self._real = threading.RLock()
            self.held = False

        def __enter__(self):
            self._real.__enter__()
            self.held = True
            return self

        def __exit__(self, *args):
            self.held = False
            return self._real.__exit__(*args)

    tracking_lock = _TrackingLock()
    orig_core_lock = core_module._client_list_iter_owners_lock
    orig_client_lock = client_module._client_list_iter_owners_lock
    core_module._client_list_iter_owners_lock = tracking_lock
    client_module._client_list_iter_owners_lock = tracking_lock
    try:
        conn = _FakeConnection([b"id=0 addr=x:1"])
        pool = _FakePool(conn)

        read_observations = []

        class _ConnectionReadTrackingRedis(redis.Redis):
            @property
            def connection(self):
                read_observations.append(tracking_lock.held)
                return self.__dict__.get("_tracked_connection")

            @connection.setter
            def connection(self, value):
                self.__dict__["_tracked_connection"] = value

        r = _ConnectionReadTrackingRedis.from_url("redis://localhost:6379/0")
        r.connection_pool = pool
        r.connection = conn

        observed_held_at_registration = []
        orig_add = weakref.WeakSet.add

        def checking_add(self_set, item):
            observed_held_at_registration.append(tracking_lock.held)
            return orig_add(self_set, item)

        weakref.WeakSet.add = checking_add
        try:
            r.client_list_iter()
        finally:
            weakref.WeakSet.add = orig_add

        assert read_observations and all(read_observations), (
            "self.connection was read while _client_list_iter_owners_lock "
            "was NOT held - close() can now run to completion in the gap "
            "between this read and this call reaching the lock, releasing "
            "the same connection this call is about to use"
        )
        assert observed_held_at_registration == [True], (
            "the new iterator was registered into "
            "_client_list_iter_owners while _client_list_iter_owners_lock "
            "was NOT held - the self.connection read-through-registration "
            "sequence in client_list_iter() is no longer atomic"
        )
    finally:
        core_module._client_list_iter_owners_lock = orig_core_lock
        client_module._client_list_iter_owners_lock = orig_client_lock


def test_redis_close_finds_iterator_registered_during_its_early_pass():
    # the specific scenario a prior round's own fix missed: close()'s
    # early, best-effort pass over pre-existing owners can take real
    # time (a slow owner's close() call); a BRAND NEW client_list_iter()
    # call, whose entire read-self.connection-through-registration
    # sequence completes entirely WHILE that early pass is still
    # running, must still be found (via a FRESH lookup, not a stale
    # snapshot) by close()'s later, self.connection-specific handling -
    # not silently released out from under it.
    #
    # A round-19 review found this test, as originally written, never
    # actually exercised that: it used a plain, non-blocking connection
    # for the registrant and fully drained (list(...)) and self-closed
    # the new iterator via registrant.join() BEFORE ever opening the
    # gate that lets close() proceed to its self.connection-specific
    # check - so by the time that check ran, the new iterator was
    # already gone regardless of whether the fix (fresh lookup) or the
    # bug (stale snapshot) was in place. Verified directly: reverting
    # the fix to reuse the stale early-pass snapshot still passed this
    # test unchanged. Rewritten below to keep the new iterator
    # genuinely open and mid-stream (via _SlowFakeConnection, with a
    # strong reference kept the whole time) while close() reaches that
    # check, and to assert on the property that actually matters -
    # whether its connection got disconnected out from under it - not
    # merely that no exception was raised.
    import redis

    conn = _SlowFakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn

    gate = threading.Event()

    class _SlowUnrelatedOwner:
        _conn = None  # unrelated to conn - doesn't affect conn_busy
        _closed = False  # real _ClientListIter instances always have this

        def close(self):
            gate.wait(timeout=5)
            self._closed = True

    # A strong reference MUST be kept somewhere: weakref.WeakSet.add()
    # only stores a weak reference, so a bare `owners.add(Foo())` with no
    # surviving reference to the temporary is garbage-collected by
    # CPython's refcounting the instant add() returns - leaving the set
    # empty and making close()'s early pass below a no-op that finishes
    # instantly, never actually stalling as this test requires.
    slow_owner = _SlowUnrelatedOwner()
    owners = weakref.WeakSet()
    owners.add(slow_owner)
    r._client_list_iter_owners = owners

    closer_done = threading.Event()

    def do_close():
        r.close()  # stalls in its early pass until the gate opens
        closer_done.set()

    closer = threading.Thread(target=do_close)
    closer.start()
    time.sleep(0.05)  # let close() get stuck in its early, unlocked pass

    # a brand-new client_list_iter() call, registering itself and
    # starting to stream entirely while close() is still stuck above -
    # deliberately left open (not fully drained) so it is still
    # genuinely mid-stream when close() reaches its self.connection
    # check below.
    results = {}

    def do_client_list_iter():
        it = r.client_list_iter()
        results["it"] = it  # strong reference - see the WeakSet-vacuity
        # comment above; letting `it` be GC'd here would itself abandon
        # the generator and disconnect conn via its own GeneratorExit
        # handling, faking the exact symptom this test is trying to
        # rule out for a different reason.
        results["first"] = next(it)

    registrant = threading.Thread(target=do_client_list_iter)
    registrant.start()
    conn.reading_started.wait(timeout=5)  # registered and mid-stream

    gate.set()  # let close()'s early pass finish and reach its own,
    # self.connection-specific handling
    closer_done.wait(timeout=5)
    closer.join()
    registrant.join(timeout=5)

    assert conn.disconnected is False
    results["it"].close()


class _OrderedOwners:
    """A drop-in replacement for the real weakref.WeakSet used by
    self._client_list_iter_owners, but with a fully deterministic
    iteration order (insertion order) instead of WeakSet's unspecified
    hash-based one - so a test can force a specific ordering rather than
    relying on it happening to occur by chance. Holds ordinary strong
    references (not weak ones); fine for a test that doesn't exercise
    GC-based cleanup."""

    def __init__(self, items):
        self._items = list(items)

    def add(self, item):
        self._items.append(item)

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)


def test_redis_close_checks_every_owner_sharing_a_connection_not_just_the_first():
    # a round-20 review found that close()'s self.connection-specific
    # owner lookup broke on the FIRST _ClientListIter whose `_conn is
    # conn`, without checking whether that one was already closed.
    # single_connection_client mode allows client_list_iter() to be
    # called more than once sequentially on the same self.connection - a
    # caller that keeps a reference to an earlier, already-fully-drained
    # call (ordinary code, not exotic misuse) keeps that stale, already-
    # closed _ClientListIter alive in the same WeakSet as a LATER,
    # genuinely still-busy one wrapping the identical connection.
    # weakref.WeakSet iteration order is unspecified, so breaking on the
    # first match could pick the stale, closed one - skipping the busy
    # check entirely for the live one and releasing/disconnecting a
    # connection still actively being read by another thread. Reproduced
    # directly against the actual (unfixed) code: with a closed and a
    # busy iterator both wrapping the same connection, natural WeakSet
    # ordering picked the closed one first in roughly half of repeated
    # trials, and each time the connection was torn down while the other
    # thread was still confirmed alive and mid-read. This test forces
    # that exact (worst-case) ordering deterministically via
    # _OrderedOwners, rather than relying on it occurring by chance.
    import redis
    from redis.commands.core import _client_list_iter_gen, _ClientListIter

    conn = _SlowFakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn

    # it1: already fully closed, but still referenced (stays "alive" in
    # the owners collection, exactly as the real WeakSet would keep it).
    it1 = _ClientListIter(pool, conn, False, _client_list_iter_gen(conn, []), r)
    it1.close()
    assert it1._closed is True

    # it2: a second call on the SAME self.connection, genuinely mid-read
    # on another thread when close() runs.
    it2 = _ClientListIter(pool, conn, False, _client_list_iter_gen(conn, []), r)
    results = {}

    def consume():
        results["rows"] = list(it2)

    t = threading.Thread(target=consume)
    t.start()
    conn.reading_started.wait(timeout=5)

    # Force the worst-case order: the stale, closed owner visited BEFORE
    # the genuinely busy one.
    r._client_list_iter_owners = _OrderedOwners([it1, it2])

    r.close()

    still_busy = t.is_alive()
    assert still_busy, "test setup broken: it2 finished before close() ran"
    assert conn.disconnected is False, (
        "close() released/disconnected a connection another thread was "
        "still genuinely mid-read on, because a stale, already-closed "
        "owner sharing the same connection was checked instead"
    )

    t.join(timeout=5)


class _SlowCloseSlowSecondLineFakeConnection(_FakeConnection):
    """Combines two independently-controllable delays on the SAME
    connection: closing a suspended (started but not exhausted)
    generator via GeneratorExit is slow (close_delay), and reading any
    line other than the first is ALSO slow (slow_read_delay) - the first
    line is always fast, so setup/registration isn't needlessly slowed
    down. Lets one connection host both "close() is stuck closing an
    abandoned owner" and "a different, freshly-registered owner is
    still genuinely mid-read" at once, as single_connection_client mode
    requires (both owners must wrap the identical connection object)."""

    def __init__(self, *args, close_delay=0.3, slow_read_delay=1.0, **kwargs):
        super().__init__(*args, **kwargs)
        self.closing_started = threading.Event()
        self.reading_started = threading.Event()
        self.close_delay = close_delay
        self.slow_read_delay = slow_read_delay

    def read_response_lines_streaming(self):
        try:
            for i, line in enumerate(self._lines):
                self.reading_started.set()
                if i > 0:
                    time.sleep(self.slow_read_delay)
                yield line
        except GeneratorExit:
            self.closing_started.set()
            time.sleep(self.close_delay)
            self.disconnect()
            raise


def test_redis_close_still_detects_owner_registered_between_its_own_lock_blocks():
    # a round-20 review found a narrower version of an earlier-fixed
    # race: close()'s self.connection handling does a fresh owner lookup
    # under one lock acquisition, then (unlocked) calls close() on
    # whatever it found, then makes its release decision under a
    # SEPARATE, later lock acquisition. A client_list_iter() call that
    # registers a brand-new owner on the exact same self.connection
    # DURING that middle, unlocked window - e.g. while an existing,
    # abandoned-but-not-yet-closed owner's own close() call is itself
    # slow - was invisible to a release decision that only trusted what
    # the earlier lookup had found. Fixed by making the release decision
    # re-scan _client_list_iter_owners fresh, under its own lock, rather
    # than trusting anything computed earlier.
    #
    # A round-21 review found the FIRST version of this test didn't
    # actually exercise that specific window: it created its delay via
    # a pre-existing owner's slow close() happening during close()'s
    # EARLY, UNLOCKED PASS (which runs before the self.connection
    # block's own fresh lookup even starts) - so by the time that fresh
    # lookup ran, the newly-registered owner was already visible to it
    # directly, never actually exercising the narrower gap between the
    # fresh-lookup lock and the release-decision lock specifically.
    # (That earlier, wider gap is legitimately covered by test_redis_
    # close_finds_iterator_registered_during_its_early_pass instead.)
    # Rewritten below to create the delay INSIDE the self.connection
    # block's OWN unlocked matching_owners-closing loop instead: an
    # unrelated owner stalls only the early pass just long enough for a
    # first same-connection owner (it2) to register and be read once
    # (fast) and then abandoned idle - idle-but-open is fine for THIS
    # owner, since it's intended to be found and closed by the
    # self.connection block's own lookup, which is what creates the
    # delay this test needs (via the connection's slow close()
    # handling). A THIRD owner (it3), genuinely busy in another thread,
    # then registers on the same connection strictly during THAT delay -
    # the exact window between the two lock blocks.
    import redis

    conn = _SlowCloseSlowSecondLineFakeConnection(
        [b"id=0 addr=x:1", b"id=1 addr=x:2"], close_delay=0.3, slow_read_delay=1.0
    )
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn

    gate = threading.Event()

    class _SlowUnrelatedOwner:
        _conn = None  # unrelated to conn - never considered by the
        # self.connection block's own lookup, only by the early pass.
        _closed = False

        def close(self):
            gate.wait(timeout=5)
            self._closed = True

    slow_unrelated_owner = _SlowUnrelatedOwner()
    owners = weakref.WeakSet()
    owners.add(slow_unrelated_owner)
    r._client_list_iter_owners = owners

    closer_done = threading.Event()

    def do_close():
        r.close()
        closer_done.set()

    closer = threading.Thread(target=do_close)
    closer.start()
    time.sleep(0.05)  # let close()'s early pass get stuck on
    # slow_unrelated_owner, well before it ever reaches the self.
    # connection block's own fresh lookup.

    # it2: registers on self.connection and is read once (fast) then
    # abandoned, WHILE the early pass is still stuck above - invisible
    # to the early pass's own (already-taken) snapshot, but genuinely
    # new and open by the time the self.connection block's fresh lookup
    # runs later.
    it2 = r.client_list_iter()
    next(it2)

    gate.set()  # let the early pass finish and reach the self.
    # connection block's own fresh lookup - which finds it2 (open,
    # idle), and its own (unlocked) attempt to close it2 is what
    # triggers the connection's slow GeneratorExit handling, creating a
    # real delay INSIDE that block specifically.
    conn.closing_started.wait(timeout=5)

    # it3: a brand-new client_list_iter() call on the SAME self.
    # connection, registering during that delay and then genuinely
    # blocking (via the connection's slow second-line read) for far
    # longer than close_delay - staying mid-read through the rest of
    # close()'s work, not just through the registration moment.
    results = {}
    it3_registered = threading.Event()

    def do_iter():
        it3 = r.client_list_iter()
        results["it3"] = it3
        it3_registered.set()
        results["first"] = next(it3)  # fast (first line)
        results["second"] = next(it3)  # slow - blocks for slow_read_delay

    registrant = threading.Thread(target=do_iter)
    registrant.start()
    assert it3_registered.wait(timeout=5), "test setup broken: it3 never registered"

    closer_done.wait(timeout=5)
    closer.join()

    # close() must not have released self.connection to the pool while
    # it3 - registered strictly after the self.connection block's own
    # fresh lookup ran, during the delay closing it2 created - was still
    # genuinely mid-read on another thread.
    assert registrant.is_alive(), "test setup broken: it3's read finished too early"
    assert conn not in pool.available, (
        "close() released the connection to the pool while a freshly-"
        "registered iterator (registered during the gap between "
        "close()'s own two lock acquisitions) was still genuinely "
        "mid-read on another thread"
    )

    registrant.join(timeout=5)
    results["it3"].close()


def test_redis_close_protection_survives_being_called_twice():
    # close() can legitimately be called more than once (e.g. a `with`
    # block's __exit__ followed by a later explicit close(), or by
    # __del__) - the busy-connection protection must not be a one-shot
    # guard that only works the first time.
    import redis

    conn = _SlowFakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    r.connection = conn

    it = r.client_list_iter()

    def consume():
        list(it)

    t = threading.Thread(target=consume)
    t.start()
    conn.reading_started.wait(timeout=5)  # deterministic, not a fixed sleep guess

    r.close()  # first call, while busy: correctly leaves conn alone
    r.close()  # second call, still busy: same - not a one-shot guard

    assert conn.disconnected is False  # still not torn down mid-read
    assert pool.available == []  # not released while still genuinely busy

    t.join()

    # once the in-flight read has actually finished, a LATER retry
    # (exactly what the _ClientListIterBusy message itself recommends)
    # must still be able to release the connection - self.connection
    # must not have been nulled out prematurely on an earlier busy call,
    # which would otherwise orphan it in the pool forever.
    r.close()
    assert pool.available == [conn]  # finally released, not leaked


def test_redis_close_protects_pooled_mode_client_list_iter_too():
    # the round-9 fix only covered self.connection (single_connection_
    # client mode) - the far more common default pooled mode, where
    # client_list_iter() checks out its own connection via
    # pool.get_connection() and self.connection stays None, had no
    # protection at all.
    import redis

    conn = _SlowFakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    # deliberately NOT setting r.connection - pooled mode

    it = r.client_list_iter()
    assert r.connection is None  # confirms pooled mode was actually used

    def consume():
        list(it)

    t = threading.Thread(target=consume)
    t.start()
    conn.reading_started.wait(timeout=5)  # deterministic, not a fixed sleep guess

    r.close()  # must not tear down the still-in-use pooled connection
    assert conn.disconnected is False

    t.join()
    assert conn.disconnected is False


def test_redis_close_protects_pooled_mode_iterator_registered_during_early_pass():
    # a round-17 review found the specific gap the test above does not
    # cover: a pooled-mode client_list_iter() call whose entire read-
    # through-registration sequence completes strictly AFTER close()'s
    # early pass snapshot is taken (but before auto_close_connection_
    # pool's final sweep) was invisible to still_in_use - because that
    # sweep used to run based on a flag computed only from the early
    # pass and, for pooled mode, a separate self.connection-only re-
    # check that never applies (self.connection stays None in pooled
    # mode). Reproduced directly against the actual current code:
    # confirmed conn.disconnected became True while a live, freshly-
    # registered iterator was still genuinely mid-stream reading from
    # it - a real cross-thread live-socket corruption. The fix makes
    # the final still_in_use decision a fresh, lock-protected scan of
    # every CURRENTLY registered owner, taken immediately before (and
    # under the same lock as) the disconnect() call itself - so a
    # registration during the gap this test creates is never missed.
    import redis

    conn0 = _FakeConnection([b"id=0 addr=x:1"])
    pool = _FakePool(conn0)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    gate = threading.Event()

    class _SlowUnrelatedOwner:
        _conn = None
        _closed = False

        def close(self):
            gate.wait(timeout=5)
            self._closed = True

    slow_owner = _SlowUnrelatedOwner()
    owners = weakref.WeakSet()
    owners.add(slow_owner)
    r._client_list_iter_owners = owners

    closer_done = threading.Event()

    def do_close():
        r.close()  # stalls in its early pass until the gate opens
        closer_done.set()

    closer = threading.Thread(target=do_close)
    closer.start()
    time.sleep(0.05)  # let close() get stuck in its early, unlocked pass

    # a brand-new POOLED-mode client_list_iter() call, registering
    # itself and starting to stream entirely while close() is stuck
    # above - self.connection stays None the whole time (pooled mode).
    conn1 = _SlowFakeConnection([b"id=1 addr=y:1", b"id=2 addr=y:2"])
    pool._conn = conn1
    pool._in_use = set()
    results = {}

    def do_iter():
        it = r.client_list_iter()
        results["it"] = it  # a strong reference MUST be kept - see the
        # WeakSet-vacuity lesson elsewhere in this file; letting `it`
        # be GC'd here would itself abandon the generator and disconnect
        # conn1 via its own GeneratorExit handling, faking the exact
        # symptom this test is trying to rule out for a different reason.
        results["first"] = next(it)

    registrant = threading.Thread(target=do_iter)
    registrant.start()
    conn1.reading_started.wait(timeout=5)  # registered and mid-stream

    gate.set()  # let close()'s early pass finish and reach its final,
    # auto_close_connection_pool sweep
    closer_done.wait(timeout=5)
    closer.join()
    registrant.join(timeout=5)

    assert conn1.disconnected is False
    results["it"].close()


class _SlowCloseFakeConnection(_FakeConnection):
    """Like _FakeConnection, but its GeneratorExit handler (triggered by
    close()) genuinely blocks for a while before finishing, so another
    thread's own next() call can race against an in-flight close(). Sets
    closing_started right as it enters that handler (rather than making
    callers guess with a fixed time.sleep) so a racing thread can
    deterministically wait for the closer to have actually entered the
    generator frame."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closing_started = threading.Event()

    def read_response_lines_streaming(self):
        try:
            yield from self._lines
        except GeneratorExit:
            self.closing_started.set()
            time.sleep(0.2)
            self.disconnect()
            raise


def test_client_list_iter_next_translates_concurrent_close_race():
    # the flip side of test_client_list_iter_concurrent_close_does_not_
    # release_in_flight_conn: here the CONSUMER's own next() call is what
    # races an in-flight close() from another thread (the watchdog/
    # cancellation pattern the docstring documents as supported) - it
    # must surface a clear RuntimeError, not a raw CPython
    # "ValueError: generator already executing".
    import redis

    conn = _SlowCloseFakeConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    it = r.client_list_iter()
    next(it)  # generator suspended at the first yield

    def closer():
        it.close()  # will block ~0.2s in GeneratorExit handling

    t = threading.Thread(target=closer)
    t.start()
    conn.closing_started.wait(timeout=5)  # deterministic, not a fixed sleep guess

    with pytest.raises(RuntimeError, match="another thread"):
        next(it)

    t.join()
    assert pool.available == [conn]  # released exactly once, no leak


def test_client_list_iter_close_releases_even_if_cleanup_raises_unrelated_error():
    # close() failing for a reason OTHER than concurrent execution (e.g.
    # disconnect() itself raising) must still release the connection -
    # only the genuine "still executing elsewhere" case should withhold it.
    class _BadDisconnectConnection(_FakeConnection):
        def read_response_lines_streaming(self):
            try:
                yield from self._lines
            except GeneratorExit:
                self.disconnect()
                raise

        def disconnect(self):
            raise RuntimeError("simulated unrelated cleanup failure")

    import redis

    conn = _BadDisconnectConnection([b"id=0 addr=x:1", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool
    # avoid an unrelated, always-raising conn.disconnect() call from
    # auto_close_connection_pool's own cleanup firing again later during
    # this test's teardown (e.g. via __del__) - not what this test is about.
    r.auto_close_connection_pool = False

    it = r.client_list_iter()
    next(it)
    with pytest.raises(RuntimeError, match="simulated unrelated cleanup failure"):
        it.close()
    assert pool.available == [conn]  # released despite the unrelated raise


def test_client_list_iter_skips_line_with_no_equals_sign():
    # a line with no '=' at all (last_key stays None the whole way
    # through _parse_client_info_fields(), redis/_parsers/helpers.py)
    # is silently dropped - the same graceful handling as a blank line
    # (see test_client_list_iter_skips_blank_line_matching_client_list)
    # - not a parse-time exception. An earlier version of this test
    # predates that tokenizer and expected a ValueError here; verified
    # directly that _parse_client_info_fields() has no raising code path
    # at all for any string input.
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1", b"this-has-no-equals-sign"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    rows = list(r.client_list_iter())
    assert rows == [{"id": "0", "addr": "x:1"}]
    assert pool.available == [conn]  # released, not leaked


def test_client_list_iter_releases_when_a_line_fails_to_decode():
    # a genuine parse-time exception (here: a byte sequence that is
    # invalid for the connection's configured, strict encoding) made
    # partway through iteration still releases the connection -
    # _client_list_iter_gen()'s `finally: inner.close()` and
    # _ClientListIter's own cleanup exist specifically for this case.
    import redis
    from redis._parsers.encoders import Encoder

    conn = _FakeConnection([b"id=0 addr=x:1", b"id=1 name=caf\xe9"])
    conn.encoder = Encoder(
        encoding="utf-8", encoding_errors="strict", decode_responses=True
    )
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    it = r.client_list_iter()
    next(it)  # the first, well-formed, valid-utf8 line
    with pytest.raises(UnicodeDecodeError):
        next(it)  # the second line's value is not valid utf-8
    assert conn.disconnected is True  # inner streaming generator was closed
    assert pool.available == [conn]  # released, not leaked


def test_client_list_iter_skips_blank_line_matching_client_list():
    # client_list_iter() must handle an embedded blank line the exact same
    # way parse_client_list() (client_list()'s own callback) does: both
    # now delegate to _parse_client_info_fields() (redis/_parsers/
    # helpers.py), which returns an empty dict for a blank/malformed line
    # rather than raising - and both silently DROP that empty dict rather
    # than surfacing it, so the two APIs return the identical record set
    # for the same underlying reply.
    import redis

    from redis._parsers.helpers import parse_client_list

    assert parse_client_list(b"id=0 addr=x:1\n\nid=1 addr=x:2\n") == [
        {"id": "0", "addr": "x:1"},
        {"id": "1", "addr": "x:2"},
    ]

    conn = _FakeConnection([b"id=0 addr=x:1", b"", b"id=1 addr=x:2"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    rows = list(r.client_list_iter())
    assert rows == [{"id": "0", "addr": "x:1"}, {"id": "1", "addr": "x:2"}]
    assert pool.available == [conn]  # released, not leaked


def test_read_response_lines_streaming_dispatches_pubsub_push():
    push = b">2\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n"
    reply, _, _ = make_reply(1)
    conn, parser = _make_resp3_connection(chunk_bytes(push + reply, 5))
    seen = []
    parser.set_pubsub_push_handler(lambda msg: seen.append(msg))

    lines = list(conn.read_response_lines_streaming())
    assert len(lines) == 1
    assert seen == [["message", "foo"]]


def test_read_response_lines_streaming_unregistered_invalidation_is_dropped():
    # no set_invalidation_push_handler() call: matches manual CLIENT
    # TRACKING ON without going through redis-py's cache=... mechanism
    push = b">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfoo\r\n"
    reply, _, _ = make_reply(1)
    conn, _parser = _make_resp3_connection(chunk_bytes(push + reply, 5))

    lines = list(conn.read_response_lines_streaming())
    assert len(lines) == 1


def test_async_client_list_iter_raises_clean_not_implemented_error():
    from redis.commands.core import AsyncManagementCommands

    class _Dummy(AsyncManagementCommands):
        pass

    with pytest.raises(NotImplementedError):
        _Dummy().client_list_iter()


def test_client_list_iter_rejects_cluster_like_client():
    import redis

    r = redis.Redis.from_url("redis://localhost:6379/0")
    saved_pool = r.connection_pool
    del r.connection_pool  # simulate a client shaped like RedisCluster
    try:
        with pytest.raises(NotImplementedError):
            r.client_list_iter()
    finally:
        r.connection_pool = saved_pool  # restore so __del__ doesn't blow up


def test_client_list_iter_rejects_unsupported_kwargs_cleanly():
    # a round-21 review found client_list_iter() didn't accept **kwargs
    # the way client_list() does (which forwards them to execute_
    # command(), where cluster-routing keywords like target_nodes are
    # consumed transparently) - so passing any keyword argument raised a
    # raw, confusing TypeError instead of the clean NotImplementedError
    # every other "not supported" case here raises.
    import redis

    conn = _FakeConnection([b"id=0 addr=x:1"])
    pool = _FakePool(conn)
    r = redis.Redis.from_url("redis://localhost:6379/0")
    r.connection_pool = pool

    with pytest.raises(NotImplementedError):
        r.client_list_iter(target_nodes="all")


def test_client_list_iter_rejects_real_cluster_classes():
    # exercises the actual production classes (not just a Redis instance
    # shaped to look like one) - the first two guards are genuinely
    # cluster/pipeline-specific (missing connection_pool / present
    # command_stack) and fire before any I/O, so no live cluster server
    # is needed here. The third (async) is NOT itself a cluster-specific
    # guard - AsyncManagementCommands.client_list_iter() unconditionally
    # raises NotImplementedError for every async client regardless of
    # self, cluster or not (see test_async_client_list_iter_raises_
    # clean_not_implemented_error) - included here only to confirm
    # async RedisCluster inherits that same override rather than
    # somehow bypassing it.
    import redis.asyncio.cluster
    import redis.cluster

    cluster = object.__new__(redis.RedisCluster)  # never sets connection_pool
    with pytest.raises(NotImplementedError):
        cluster.client_list_iter()

    pipe = object.__new__(redis.cluster.ClusterPipeline)
    pipe.command_stack = []
    with pytest.raises(NotImplementedError):
        pipe.client_list_iter()

    async_cluster = object.__new__(redis.asyncio.cluster.RedisCluster)
    with pytest.raises(NotImplementedError):
        async_cluster.client_list_iter()
