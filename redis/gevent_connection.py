"""
Helper for gevent socket.
"""

from gevent import socket
from . import connection

class Connection(connection.Connection):
    
    def _connect(self):
        "Create a TCP socket connection"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        sock.connect((self.host, self.port))
        return sock
