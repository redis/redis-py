import asyncio
import re
import socket
import ssl
import struct
import sys

import pytest
from redis.asyncio.connection import (
    Connection,
    SSLConnection,
    UnixDomainSocketConnection,
)
from redis.exceptions import ConnectionError

from ..ssl_utils import CertificateType, get_tls_certificates

_CLIENT_NAME = "test-suite-client"
_CMD_SEP = b"\r\n"
_SUCCESS_RESP = b"+OK" + _CMD_SEP
_PONG_RESP = b"+PONG" + _CMD_SEP
_ERROR_RESP = b"-ERR" + _CMD_SEP
_SUPPORTED_CMDS = {f"CLIENT SETNAME {_CLIENT_NAME}": _SUCCESS_RESP}


@pytest.fixture
def tcp_address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()


@pytest.fixture
def uds_address(tmpdir):
    return tmpdir / "uds.sock"


@pytest.mark.parametrize(
    "check_server_ready",
    [True, False],
    ids=["check_server_ready", "no_check_server_ready"],
)
async def test_tcp_connect(tcp_address, check_server_ready):
    host, port = tcp_address
    conn = Connection(
        host=host,
        port=port,
        client_name=_CLIENT_NAME,
        socket_timeout=10,
        check_server_ready=check_server_ready,
    )
    await _assert_connect(conn, tcp_address, check_server_ready)


@pytest.mark.parametrize(
    "check_server_ready",
    [True, False],
    ids=["check_server_ready", "no_check_server_ready"],
)
async def test_uds_connect(uds_address, check_server_ready):
    path = str(uds_address)
    conn = UnixDomainSocketConnection(
        path=path,
        client_name=_CLIENT_NAME,
        socket_timeout=10,
        check_server_ready=check_server_ready,
    )
    await _assert_connect(conn, path, check_server_ready)


@pytest.mark.ssl
@pytest.mark.parametrize(
    "ssl_ciphers",
    [
        "AES256-SHA:DHE-RSA-AES256-SHA:AES128-SHA:DHE-RSA-AES128-SHA",
        "ECDHE-ECDSA-AES256-GCM-SHA384",
        "ECDHE-RSA-AES128-GCM-SHA256",
    ],
)
async def test_tcp_ssl_tls12_custom_ciphers(tcp_address, ssl_ciphers):
    host, port = tcp_address

    # in order to have working hostname verification, we need to use "localhost"
    # as redis host as the server certificate is self-signed and only valid for "localhost"
    host = "localhost"

    server_certs = get_tls_certificates(cert_type=CertificateType.server)

    conn = SSLConnection(
        host=host,
        port=port,
        client_name=_CLIENT_NAME,
        ssl_ca_certs=server_certs.ca_certfile,
        socket_timeout=10,
        ssl_min_version=ssl.TLSVersion.TLSv1_2,
        ssl_ciphers=ssl_ciphers,
    )
    await _assert_connect(
        conn, tcp_address, certfile=server_certs.certfile, keyfile=server_certs.keyfile
    )
    await conn.disconnect()


@pytest.mark.ssl
@pytest.mark.parametrize(
    "ssl_min_version",
    [
        ssl.TLSVersion.TLSv1_2,
        pytest.param(
            ssl.TLSVersion.TLSv1_3,
            marks=pytest.mark.skipif(not ssl.HAS_TLSv1_3, reason="requires TLSv1.3"),
        ),
    ],
)
@pytest.mark.parametrize(
    "check_server_ready",
    [True, False],
    ids=["check_server_ready", "no_check_server_ready"],
)
async def test_tcp_ssl_connect(tcp_address, ssl_min_version, check_server_ready):
    host, port = tcp_address

    # in order to have working hostname verification, we need to use "localhost"
    # as redis host as the server certificate is self-signed and only valid for "localhost"
    host = "localhost"

    server_certs = get_tls_certificates(cert_type=CertificateType.server)

    conn = SSLConnection(
        host=host,
        port=port,
        client_name=_CLIENT_NAME,
        ssl_ca_certs=server_certs.ca_certfile,
        socket_timeout=10,
        ssl_min_version=ssl_min_version,
        check_server_ready=check_server_ready,
    )
    await _assert_connect(
        conn,
        tcp_address,
        check_server_ready,
        certfile=server_certs.certfile,
        keyfile=server_certs.keyfile,
    )
    await conn.disconnect()


@pytest.mark.asyncio
async def test_connect_check_server_ready_asyncio_timeout_error(tcp_address):
    """
    Demonstrates a scenario where redis-py hits an `asyncio.TimeoutError` internally
    (via `asyncio.wait_for(...)` or `async_timeout(...)`). Redis-py catches that
    and re-raises `ConnectionError`.
    """
    host, port = tcp_address
    conn = Connection(
        host=host,
        port=port,
        client_name="test-suite-client",
        socket_timeout=0.5,
        check_server_ready=True,
    )

    async def _no_response_handler(reader, writer):
        # Accept the connection
        buffer = await reader.read(1024)
        assert "PING" in buffer.decode()
        # do nothing (no response to PING).
        # The client code will eventually hit asyncio.TimeoutError on read.
        await asyncio.sleep(1)
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(_no_response_handler, host=host, port=port)
    async with server:
        await server.start_serving()
        # We expect ConnectionError due to the underlying asyncio.TimeoutError
        # from lack of a timely PONG.
        with pytest.raises(ConnectionError):
            await conn.connect()

        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_connect_check_server_ready_invalid_ping(tcp_address):
    """
    Demonstrates a scenario where redis-py hits an `ResponseError` internally
    due to an invalid response to the PING command.
    Redis-py catches that and re-raises `ConnectionError`.
    """
    host, port = tcp_address
    conn = Connection(
        host=host,
        port=port,
        client_name="test-suite-client",
        socket_timeout=5,
        check_server_ready=True,
    )

    async def _no_response_handler(reader, writer):
        # Accept the connection
        buffer = await reader.read(1024)
        assert "PING" in buffer.decode()
        # Send wrong answer back
        writer.write(_ERROR_RESP)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(_no_response_handler, host=host, port=port)
    async with server:
        await server.start_serving()
        # We expect ConnectionError due to a wrong response to PING.
        with pytest.raises(ConnectionError, match="Invalid PING response"):
            await conn.connect()

        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_connect_check_server_ready_connection_reset(tcp_address):
    """
    Demonstrates a scenario where the server accepts the connection and receives the PING command,
    but abruptly resets the connection (sends a TCP RST). This causes the client to raise a
    ConnectionResetError internally, which redis-py catches and re-raises as ConnectionError.
    """
    host, port = tcp_address
    conn = Connection(
        host=host,
        port=port,
        client_name="test-suite-client",
        socket_timeout=5,
        check_server_ready=True,
    )

    async def _no_response_handler(reader, writer):
        # Accept the connection
        buffer = await reader.read(1024)
        assert "PING" in buffer.decode()

        sock = writer.transport.get_extra_info("socket")

        # Configure socket for abrupt close (RST packet)
        linger_struct = struct.pack("ii", 1, 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger_struct)

        # Close immediately to trigger ConnectionResetError on client side
        sock.close()

    server = await asyncio.start_server(_no_response_handler, host=host, port=port)
    async with server:
        await server.start_serving()

        if sys.version_info < (3, 11, 3):
            # Python 3.11.3+ handles ConnectionResetError differently
            # and overloads with TimeoutError.
            with pytest.raises(ConnectionError):
                await conn.connect()
        else:
            with pytest.raises(ConnectionError, match="Connection reset by peer"):
                await conn.connect()

        server.close()
        await server.wait_closed()


@pytest.mark.ssl
@pytest.mark.skipif(not ssl.HAS_TLSv1_3, reason="requires TLSv1.3")
async def test_tcp_ssl_version_mismatch(tcp_address):
    host, port = tcp_address
    certfile, keyfile, _ = get_tls_certificates(cert_type=CertificateType.server)
    conn = SSLConnection(
        host=host,
        port=port,
        client_name=_CLIENT_NAME,
        ssl_ca_certs=certfile,
        socket_timeout=1,
        ssl_min_version=ssl.TLSVersion.TLSv1_3,
    )
    with pytest.raises(ConnectionError):
        await _assert_connect(
            conn,
            tcp_address,
            certfile=certfile,
            keyfile=keyfile,
            maximum_ssl_version=ssl.TLSVersion.TLSv1_2,
        )
    await conn.disconnect()


async def _assert_connect(
    conn,
    server_address,
    check_server_ready=False,
    certfile=None,
    keyfile=None,
    minimum_ssl_version=ssl.TLSVersion.TLSv1_2,
    maximum_ssl_version=ssl.TLSVersion.TLSv1_3,
):
    stop_event = asyncio.Event()
    finished = asyncio.Event()

    async def _handler(reader, writer):
        try:
            return await _redis_request_handler(
                reader, writer, stop_event, check_server_ready
            )
        finally:
            writer.close()
            await writer.wait_closed()
            finished.set()

    if isinstance(server_address, str):
        server = await asyncio.start_unix_server(_handler, path=server_address)
    elif certfile:
        host, port = server_address
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.minimum_version = minimum_ssl_version
        context.maximum_version = maximum_ssl_version
        context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        server = await asyncio.start_server(_handler, host=host, port=port, ssl=context)
    else:
        host, port = server_address
        server = await asyncio.start_server(_handler, host=host, port=port)

    async with server as aserver:
        await aserver.start_serving()
        try:
            await conn.connect()
            await conn.disconnect()
        except ConnectionError:
            finished.set()
            raise
        finally:
            stop_event.set()
            aserver.close()
            await aserver.wait_closed()
            await finished.wait()


async def _redis_request_handler(reader, writer, stop_event, check_server_ready):
    command = None
    command_ptr = None
    fragment_length = None
    while not stop_event.is_set():
        buffer = await reader.read(1024)
        if not buffer:
            break
        parts = re.split(_CMD_SEP, buffer)
        for fragment in parts:
            fragment = fragment.decode()
            if not fragment:
                continue

            if fragment.startswith("*") and command is None:
                command = [None for _ in range(int(fragment[1:]))]
                command_ptr = 0
                fragment_length = None
                continue

            if fragment.startswith("$") and command[command_ptr] is None:
                fragment_length = int(fragment[1:])
                continue

            assert len(fragment) == fragment_length
            command[command_ptr] = fragment
            command_ptr += 1

            if command_ptr < len(command):
                continue

            command = " ".join(command)
            if check_server_ready and command == "PING":
                resp = _PONG_RESP
            else:
                resp = _SUPPORTED_CMDS.get(command, _ERROR_RESP)
            writer.write(resp)
            await writer.drain()
            command = None
