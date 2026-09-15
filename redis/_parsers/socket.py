import errno
import io
import socket
from io import SEEK_END
from typing import Optional, Union

from ..exceptions import ConnectionError, TimeoutError
from ..utils import SENTINEL, SSL_AVAILABLE

NONBLOCKING_EXCEPTION_ERROR_NUMBERS = {BlockingIOError: errno.EWOULDBLOCK}

if SSL_AVAILABLE:
    import ssl

    if hasattr(ssl, "SSLWantReadError"):
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantReadError] = 2
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantWriteError] = 2
    else:
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLError] = 2

NONBLOCKING_EXCEPTIONS = tuple(NONBLOCKING_EXCEPTION_ERROR_NUMBERS.keys())

SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."

SYM_CRLF = b"\r\n"


class SocketBuffer:
    def __init__(
        self, socket: socket.socket, socket_read_size: int, socket_timeout: float
    ):
        self._sock = socket
        self.socket_read_size = socket_read_size
        self.socket_timeout = socket_timeout
        self._buffer = io.BytesIO()

    def _live_buffer(self) -> io.BytesIO:
        """
        The read buffer, or a ConnectionError when the connection is already gone.

        ``close()`` closes the buffer before dropping it, and it can run while another
        thread reads: the multi-database client closes connections from its health
        check thread when a database is taken out of service, so a reader finds either
        a closed ``BytesIO`` or ``None``. The connection is gone either way, so report
        it the way every other teardown here is reported and let the retry layers act
        on it, instead of surfacing ``ValueError: I/O operation on closed file`` to the
        caller of the command.
        """
        buffer = self._buffer

        if buffer is None or buffer.closed:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        return buffer

    def unread_bytes(self) -> int:
        """
        Remaining unread length of buffer
        """
        buffer = self._live_buffer()

        try:
            pos = buffer.tell()
            end = buffer.seek(0, SEEK_END)
            buffer.seek(pos)
        except ValueError:
            # Closed between the check above and here.
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

        return end - pos

    def _read_from_socket(
        self,
        length: Optional[int] = None,
        timeout: Union[float, object] = SENTINEL,
        raise_on_timeout: Optional[bool] = True,
    ) -> bool:
        sock = self._sock
        socket_read_size = self.socket_read_size
        marker = 0
        custom_timeout = timeout is not SENTINEL

        buf = self._live_buffer()

        if sock is None:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        try:
            current_pos = buf.tell()
            buf.seek(0, SEEK_END)
        except ValueError:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

        if custom_timeout:
            sock.settimeout(timeout)
        try:
            while True:
                data = sock.recv(socket_read_size)
                # an empty string indicates the server shutdown the socket
                if isinstance(data, bytes) and len(data) == 0:
                    raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
                buf.write(data)
                data_length = len(data)
                marker += data_length

                if length is not None and length > marker:
                    continue
                return True
        except socket.timeout:
            if raise_on_timeout:
                raise TimeoutError("Timeout reading from socket")
            return False
        except NONBLOCKING_EXCEPTIONS as ex:
            # if we're in nonblocking mode and the recv raises a
            # blocking error, simply return False indicating that
            # there's no data to be read. otherwise raise the
            # original exception.
            allowed = NONBLOCKING_EXCEPTION_ERROR_NUMBERS.get(ex.__class__, -1)
            if ex.errno == allowed:
                if not raise_on_timeout:
                    return False
                if timeout == 0:
                    raise TimeoutError("Timeout reading from socket")
            raise ConnectionError(f"Error while reading from socket: {ex.args}")
        except ValueError:
            # The buffer was closed by another thread while this read was in flight.
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None
        finally:
            try:
                buf.seek(current_pos)
            except ValueError:
                # Closed by another thread while recv was blocked. Whatever the body
                # of the read raised is the outcome to report, so this stays quiet
                # rather than replacing it - the next read raises ConnectionError.
                pass
            if custom_timeout:
                try:
                    sock.settimeout(self.socket_timeout)
                except OSError:
                    # Same window as the seek above: the close that dropped the
                    # buffer closed the socket too, so there is nothing left to
                    # restore the timeout on. Staying quiet keeps the outcome the
                    # body reported instead of replacing it with EBADF.
                    pass

    def can_read(self, timeout: float = 0) -> bool:
        return bool(self.unread_bytes()) or self._read_from_socket(
            timeout=timeout, raise_on_timeout=False
        )

    def read(self, length: int, timeout: Union[float, object] = SENTINEL) -> bytes:
        length = length + 2  # make sure to read the \r\n terminator
        buf = self._live_buffer()
        try:
            # BufferIO will return less than requested if buffer is short
            data = buf.read(length)
            missing = length - len(data)
            if missing:
                # fill up the buffer and read the remainder
                self._read_from_socket(length=missing, timeout=timeout)
                data += buf.read(missing)
        except ValueError:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None
        return data[:-2]

    def readline(self, timeout: Union[float, object] = SENTINEL) -> bytes:
        buf = self._live_buffer()
        try:
            data = buf.readline()
            while not data.endswith(SYM_CRLF):
                # there's more data in the socket that we need
                self._read_from_socket(timeout=timeout)
                data += buf.readline()
        except ValueError:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

        return data[:-2]

    # Safety valve for read_bulk_lines(): a single record with no '\n' in
    # it would otherwise grow `pending` without bound. This caps the
    # damage a single pathological record (e.g. an extremely long CLIENT
    # SETNAME/SETINFO value) can do to a small, fixed multiple of this -
    # instead of the full reply. This is a nominal, not exact, bound:
    # `pending += chunk` always allocates a new bytes object (bytes,
    # unlike str, has no in-place-growth optimization in CPython), so
    # the old and new buffers are transiently both live around the
    # addition that finally crosses this threshold - measured peak
    # Python-level memory at that instant is roughly 2x this value, not
    # capped tightly at it.
    MAX_UNTERMINATED_LINE_SIZE = 16 * 1024 * 1024

    def read_bulk_lines(self, length: int):
        """
        Stream a RESP bulk-string payload of exactly `length` bytes as
        newline (``\\n``)-delimited lines, without accumulating the full
        payload into `self._buffer`. Yields each line's bytes with any
        trailing ``\\r`` stripped.

        Unlike `read()`, at most `socket_read_size` bytes of payload are
        held in memory per normal line, plus one pending, not-yet-terminated
        line - this is meant for replies such as CLIENT LIST that pack many
        independent records into a single giant bulk string, so memory use
        stays independent of how many records there are. A single record
        with no line terminator in it is still bounded, but only by
        `MAX_UNTERMINATED_LINE_SIZE`, not by `socket_read_size` - such a
        record raises ConnectionError rather than growing unbounded.

        Consumes exactly `length` payload bytes plus the trailing CRLF
        reply terminator, leaving the buffer positioned at the start of
        the next reply - callers MUST fully exhaust the generator (or
        disconnect the underlying socket) before issuing another command,
        the same way an abandoned partial `read()` would leave the
        connection desynchronized.
        """
        if length < 0:
            raise ConnectionError(f"read_bulk_lines() got a negative length: {length}")
        remaining = length
        # `pending` accumulates the current, not-yet-terminated record
        # across chunks. It's a bytearray so `pending += ...` is
        # amortized O(1) per append (CPython over-allocates bytearrays
        # the same way it does lists), never a full O(len(pending))
        # copy the way plain `bytes += bytes` always is.
        #
        # Just as important, and easy to miss: this loop deliberately
        # searches for '\n' ONLY within each newly-arrived `chunk`
        # (`b"\n" in chunk`, `chunk.split(b"\n")`), never by re-scanning
        # the whole, potentially large `pending` accumulator itself
        # (e.g. `b"\n" in pending`). That invariant - pending never
        # contains '\n' at the top of the loop, either because it's
        # freshly split with none remaining, or still empty - is what
        # makes each iteration's work proportional to len(chunk), not to
        # how large pending has grown so far. An earlier version of this
        # method got the bytearray part right but still re-scanned all
        # of `pending` on every iteration for '\n' - nothing controls
        # how few bytes a single recv() call returns, so a
        # slow/adversarial peer trickling an unterminated record back a
        # handful of bytes at a time (nothing rules this out - it's the
        # same peer nothing here can constrain) drove that repeated
        # whole-buffer scan to genuinely quadratic total cost as pending
        # grew toward MAX_UNTERMINATED_LINE_SIZE, before that safety
        # valve even fires - directly undermining that valve's whole
        # purpose (bounding the damage from a pathological record).
        # Reproduced directly: with 1-byte-at-a-time chunks, both the
        # naive `bytes += chunk` version AND a `bytearray += chunk`
        # version that still re-scanned all of `pending` for '\n' each
        # time took minutes to accumulate a few hundred KB - clearly
        # superlinear either way. Every VALUE this method yields is
        # still plain `bytes` (via the `bytes(...)` calls below) - only
        # the internal accumulator, and where the '\n' search looks, are
        # different.
        pending = bytearray()
        first_chunk = True
        buf = self._live_buffer()
        while True:
            if first_chunk:
                try:
                    chunk = buf.read(remaining)
                except ValueError:
                    raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None
                first_chunk = False
            elif remaining <= 0:
                break
            else:
                chunk = self._recv_capped(min(self.socket_read_size, remaining))
            remaining -= len(chunk)

            if b"\n" not in chunk:
                pending += chunk
                # Checked every iteration this branch is taken: a single
                # record with no line terminator in it is bounded, but
                # only by MAX_UNTERMINATED_LINE_SIZE, not by
                # socket_read_size - such a record raises ConnectionError
                # here rather than growing unbounded. Measured on the
                # trailing-'\r'-stripped length, to match what the
                # eventual yield (either the post-loop tail below, or
                # the completed-first-line case just below once a '\n'
                # finally arrives) actually produces - checking the raw,
                # not-yet-stripped length would reject a record whose
                # real content is exactly MAX_UNTERMINATED_LINE_SIZE
                # bytes as "exceeded", even though it has not.
                pending_len = len(pending) - (1 if pending.endswith(b"\r") else 0)
                if pending_len > self.MAX_UNTERMINATED_LINE_SIZE:
                    raise ConnectionError(
                        "A CLIENT LIST record exceeded "
                        f"{self.MAX_UNTERMINATED_LINE_SIZE} bytes without "
                        "a line terminator; aborting streaming read"
                    )
                continue

            # `chunk` has at least one '\n' - and, by the loop's own
            # invariant, `pending` (whatever was accumulated so far)
            # has none - so every '\n' in "pending + chunk" is located
            # within `chunk` itself; splitting just `chunk` (bounded by
            # len(chunk), not by how large pending has grown) is
            # therefore sufficient and correct, never a whole-buffer
            # rescan.
            parts = chunk.split(b"\n")
            pending += parts[0]  # completes whatever was pending before
            complete_lines = [bytes(pending)] + [bytes(p) for p in parts[1:-1]]
            pending = bytearray(parts[-1])
            for line in complete_lines:
                # Same reasoning and same trailing-'\r'-stripped
                # measurement as the unterminated-accumulation check
                # above - see that comment.
                stripped = line[:-1] if line.endswith(b"\r") else line
                if len(stripped) > self.MAX_UNTERMINATED_LINE_SIZE:
                    raise ConnectionError(
                        "A CLIENT LIST record exceeded "
                        f"{self.MAX_UNTERMINATED_LINE_SIZE} bytes; "
                        "aborting streaming read"
                    )
                yield stripped
            pending_len = len(pending) - (1 if pending.endswith(b"\r") else 0)
            if pending_len > self.MAX_UNTERMINATED_LINE_SIZE:
                raise ConnectionError(
                    "A CLIENT LIST record exceeded "
                    f"{self.MAX_UNTERMINATED_LINE_SIZE} bytes without a "
                    "line terminator; aborting streaming read"
                )
        if pending:
            final = bytes(pending)
            yield final[:-1] if final.endswith(b"\r") else final
        self._consume(2)  # discard the trailing CRLF reply terminator

    def _recv_capped(self, max_length: int) -> bytes:
        """Read at most `max_length` bytes directly from the socket."""
        sock = self._sock
        if sock is None:
            # closed by another thread between this call and the last -
            # same condition _read_from_socket() itself already guards
            # against, for the same reason.
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        try:
            data = sock.recv(max_length)
        except socket.timeout:
            raise TimeoutError("Timeout reading from socket")
        except NONBLOCKING_EXCEPTIONS as ex:
            raise ConnectionError(f"Error while reading from socket: {ex.args}")
        if not data:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        return data

    def _consume(self, length: int) -> None:
        """Read and discard exactly `length` bytes."""
        try:
            remaining = length - len(self._live_buffer().read(length))
        except ValueError:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None
        while remaining > 0:
            remaining -= len(self._recv_capped(remaining))

    def get_pos(self) -> int:
        """
        Get current read position
        """
        try:
            return self._live_buffer().tell()
        except ValueError:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

    def rewind(self, pos: int) -> None:
        """
        Rewind the buffer to a specific position, to re-start reading
        """
        buffer = self._buffer

        # Best effort: the caller is unwinding a read that already failed, and a
        # buffer closed by another thread has nothing to rewind. Raising here would
        # replace the exception the caller is propagating.
        if buffer is None:
            return

        try:
            buffer.seek(pos)
        except ValueError:
            pass

    def purge(self) -> None:
        """
        After a successful read, purge the read part of buffer
        """
        try:
            self._purge()
        except (ConnectionError, ValueError):
            # Closed by another thread while the response was being read. The
            # response is already parsed, so there is nothing to report and nothing
            # left to purge.
            return

    def _purge(self) -> None:
        unread = self.unread_bytes()

        # Only if we have read all of the buffer do we truncate, to
        # reduce the amount of memory thrashing.  This heuristic
        # can be changed or removed later.
        if unread > 0:
            return

        # Bind the buffer once: another thread's ``close()`` can drop
        # ``self._buffer`` to None between the read above and the truncate below,
        # and the resulting AttributeError would escape ``purge()``'s best effort
        # wrapper. A local reference to an already closed buffer raises
        # ValueError, which ``purge()`` handles.
        buffer = self._live_buffer()

        if unread > 0:
            # move unread data to the front
            view = buffer.getbuffer()
            view[:unread] = view[-unread:]
        buffer.truncate(unread)
        buffer.seek(0)

    def close(self) -> None:
        try:
            self._buffer.close()
        except Exception:
            # issue #633 suggests the purge/close somehow raised a
            # BadFileDescriptor error. Perhaps the client ran out of
            # memory or something else? It's probably OK to ignore
            # any error being raised from purge/close since we're
            # removing the reference to the instance below.
            pass
        self._buffer = None
        self._sock = None
