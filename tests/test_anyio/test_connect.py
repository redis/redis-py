import re
import socket
import ssl
import sys

import anyio
import pytest
from anyio.abc import ByteStream
from anyio.streams.tls import TLSListener

from redis.anyio.connection import (
    Connection,
    SSLConnection,
    UnixDomainSocketConnection,
)
from redis.exceptions import ConnectionError

from ..ssl_utils import CertificateType, get_tls_certificates

if sys.version_info < (3, 11):
    from exceptiongroup import ExceptionGroup

pytestmark = pytest.mark.anyio

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
async def test_tcp_ssl_connect(tcp_address, ssl_min_version):
    host, port = tcp_address

    server_certs = get_tls_certificates(cert_type=CertificateType.server)

    conn = SSLConnection(
        host=host,
        port=port,
        client_name=_CLIENT_NAME,
        ssl_ca_certs=server_certs.ca_certfile,
        socket_timeout=10,
        ssl_min_version=ssl_min_version,
    )
    await _assert_connect(
        conn, tcp_address, certfile=server_certs.certfile, keyfile=server_certs.keyfile
    )
    await conn.disconnect()


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
    certfile=None,
    keyfile=None,
    minimum_ssl_version=ssl.TLSVersion.TLSv1_2,
    maximum_ssl_version=ssl.TLSVersion.TLSv1_3,
):
    stop_event = anyio.Event()
    finished = anyio.Event()

    async def _handler(stream: ByteStream):
        try:
            async with stream:
                return await _redis_request_handler(stream, stop_event)
        finally:
            finished.set()

    if isinstance(server_address, str):
        listener = await anyio.create_unix_listener(path=server_address)
    elif certfile:
        host, port = server_address
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.minimum_version = minimum_ssl_version
        context.maximum_version = maximum_ssl_version
        context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        tcp_listener = await anyio.create_tcp_listener(local_host=host, local_port=port)
        listener = TLSListener(
            tcp_listener, ssl_context=context, standard_compatible=False
        )
    else:
        host, port = server_address
        listener = await anyio.create_tcp_listener(local_host=host, local_port=port)

    try:
        async with listener, anyio.create_task_group() as tg:
            tg.start_soon(listener.serve, _handler)
            try:
                await conn.connect()
                await conn.disconnect()
            except ConnectionError:
                finished.set()
                raise
            finally:
                stop_event.set()
                await finished.wait()
                tg.cancel_scope.cancel()
    except ExceptionGroup as excgrp:
        if len(excgrp.exceptions) == 1 and isinstance(
            excgrp.exceptions[0], ConnectionError
        ):
            raise excgrp.exceptions[0] from None

        raise


async def _redis_request_handler(stream: ByteStream, stop_event: anyio.Event):
    command = None
    command_ptr = None
    fragment_length = None
    while not stop_event.is_set():
        try:
            buffer = await stream.receive()
        except anyio.EndOfStream:
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
            resp = _SUPPORTED_CMDS.get(command, _ERROR_RESP)
            await stream.send(resp)
            command = None
