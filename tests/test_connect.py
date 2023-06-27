import logging
import re
import socket
import socketserver
import ssl
import threading

import pytest

from redis.connection import Connection, SSLConnection, UnixDomainSocketConnection

from .ssl_utils import get_ssl_filename

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


def test_tcp_connect(tcp_address):
    host, port = tcp_address
    conn = Connection(host=host, port=port, client_name=_CLIENT_NAME, socket_timeout=10)
    _assert_connect(conn, tcp_address)


def test_uds_connect(uds_address):
    path = str(uds_address)
    conn = UnixDomainSocketConnection(path, client_name=_CLIENT_NAME, socket_timeout=10)
    _assert_connect(conn, path)


@pytest.mark.ssl
def test_tcp_ssl_connect(tcp_address):
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
    _assert_connect(conn, tcp_address, certfile=certfile, keyfile=keyfile)


def _assert_connect(conn, server_address, certfile=None, keyfile=None):
    if isinstance(server_address, str):
        server = _RedisUDSServer(server_address, _RedisRequestHandler)
    else:
        server = _RedisTCPServer(
            server_address, _RedisRequestHandler, certfile=certfile, keyfile=keyfile
        )
    with server as aserver:
        t = threading.Thread(target=aserver.serve_forever)
        t.start()
        try:
            aserver.wait_online()
            conn.connect()
            conn.disconnect()
        finally:
            aserver.stop()
            t.join(timeout=5)


class _RedisTCPServer(socketserver.TCPServer):
    def __init__(self, *args, certfile=None, keyfile=None, **kw) -> None:
        self._ready_event = threading.Event()
        self._stop_requested = False
        self._certfile = certfile
        self._keyfile = keyfile
        super().__init__(*args, **kw)

    def service_actions(self):
        self._ready_event.set()

    def wait_online(self):
        self._ready_event.wait()

    def stop(self):
        self._stop_requested = True
        self.shutdown()

    def is_serving(self):
        return not self._stop_requested

    def get_request(self):
        if self._certfile is None:
            return super().get_request()
        newsocket, fromaddr = self.socket.accept()
        connstream = ssl.wrap_socket(
            newsocket,
            server_side=True,
            certfile=self._certfile,
            keyfile=self._keyfile,
            ssl_version=ssl.PROTOCOL_TLSv1_2,
        )
        return connstream, fromaddr


class _RedisUDSServer(socketserver.UnixStreamServer):
    def __init__(self, *args, **kw) -> None:
        self._ready_event = threading.Event()
        self._stop_requested = False
        super().__init__(*args, **kw)

    def service_actions(self):
        self._ready_event.set()

    def wait_online(self):
        self._ready_event.wait()

    def stop(self):
        self._stop_requested = True
        self.shutdown()

    def is_serving(self):
        return not self._stop_requested


class _RedisRequestHandler(socketserver.StreamRequestHandler):
    def setup(self):
        _logger.info("%s connected", self.client_address)

    def finish(self):
        _logger.info("%s disconnected", self.client_address)

    def handle(self):
        buffer = b""
        command = None
        command_ptr = None
        fragment_length = None
        while self.server.is_serving() or buffer:
            try:
                buffer += self.request.recv(1024)
            except socket.timeout:
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
                _logger.info("Response %s", resp)
                self.request.sendall(resp)
                command = None
        _logger.info("Exit handler")
