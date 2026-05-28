"""Internal default helper functions shared across redis-py modules."""

import socket

# Connection defaults

DEFAULT_SOCKET_TIMEOUT = 5  # 5s
DEFAULT_SOCKET_CONNECT_TIMEOUT = DEFAULT_SOCKET_TIMEOUT
DEFAULT_SOCKET_READ_SIZE = 32768  # 32KB


def get_default_socket_keepalive_options() -> dict[int, int]:
    options = {}

    # Linux exposes TCP_KEEPIDLE; macOS exposes the equivalent TCP_KEEPALIVE.
    # Some platforms expose neither and only support SO_KEEPALIVE itself.
    tcp_keepidle = getattr(socket, "TCP_KEEPIDLE", None)
    if tcp_keepidle is None:
        tcp_keepidle = getattr(socket, "TCP_KEEPALIVE", None)
    if tcp_keepidle is not None:
        options[tcp_keepidle] = 30

    # Not every platform exposes interval/probe tuning constants.
    tcp_keepintvl = getattr(socket, "TCP_KEEPINTVL", None)
    if tcp_keepintvl is not None:
        options[tcp_keepintvl] = 5

    # Not every platform exposes interval/probe tuning constants.
    tcp_keepcnt = getattr(socket, "TCP_KEEPCNT", None)
    if tcp_keepcnt is not None:
        options[tcp_keepcnt] = 3

    return options


# Retry defaults
DEFAULT_RETRY_COUNT = 10
DEFAULT_RETRY_BASE = 0.01  # 10ms
DEFAULT_RETRY_CAP = 1  # 1s
