import asyncio
import logging
import re
import socket
import ssl

import pytest

from redis.asyncio.connection import (
    Connection,
    SSLConnection,
    UnixDomainSocketConnection,
)

from ..ssl_utils import get_ssl_filename

_logger = logging.getLogger(__name__)


_CLIENT_NAME = "test-suite-client"
_CMD_SEP = b"\r\n"
_SUCCESS_RESP = b"+OK" + _CMD_SEP
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


async def test_tcp_connect(tcp_address):
    host, port = tcp_address
    conn = Connection(host=host, port=port, client_name=_CLIENT_NAME, socket_timeout=10)
    await _assert_connect(conn, tcp_address)


async def test_uds_connect(uds_address):
    path = str(uds_address)
    conn = UnixDomainSocketConnection(
        path=path, client_name=_CLIENT_NAME, socket_timeout=10
    )
    await _assert_connect(conn, path)


@pytest.mark.ssl
async def test_tcp_ssl_connect(tcp_address):
    host, port = tcp_address
    certfile = get_ssl_filename("server-cert.pem")
    keyfile = get_ssl_filename("server-key.pem")
    conn = SSLConnection(
        host=host,
        port=port,
        client_name=_CLIENT_NAME,
        ssl_ca_certs=certfile,
        socket_timeout=10,
    )
    await _assert_connect(conn, tcp_address, certfile=certfile, keyfile=keyfile)


async def _assert_connect(conn, server_address, certfile=None, keyfile=None):
    stop_event = asyncio.Event()
    finished = asyncio.Event()

    async def _handler(reader, writer):
        try:
            return await _redis_request_handler(reader, writer, stop_event)
        finally:
            finished.set()

    if isinstance(server_address, str):
        server = await asyncio.start_unix_server(_handler, path=server_address)
    elif certfile:
        host, port = server_address
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
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
        finally:
            stop_event.set()
            aserver.close()
            await aserver.wait_closed()
            await finished.wait()


async def _redis_request_handler(reader, writer, stop_event):
    buffer = b""
    command = None
    command_ptr = None
    fragment_length = None
    while not stop_event.is_set() or buffer:
        _logger.info(str(stop_event.is_set()))
        try:
            buffer += await asyncio.wait_for(reader.read(1024), timeout=0.5)
        except TimeoutError:
            continue
        if not buffer:
            continue
        parts = re.split(_CMD_SEP, buffer)
        buffer = parts[-1]
        for fragment in parts[:-1]:
            fragment = fragment.decode()
            _logger.info("Command fragment: %s", fragment)

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
            _logger.info("Command %s", command)
            resp = _SUPPORTED_CMDS.get(command, _ERROR_RESP)
            _logger.info("Response from %s", resp)
            writer.write(resp)
            await writer.drain()
            command = None
    _logger.info("Exit handler")
