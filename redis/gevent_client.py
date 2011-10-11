"""
Helper for gevent socket.
"""

from gevent import socket

from redis import connection
from redis import client

class Connection(connection.Connection):
    
    def _connect(self):
        "Create a TCP socket connection"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        sock.connect((self.host, self.port))
        return sock


class Redis(client.Redis):
    def __init__(self, host='localhost', port=6379,
                 db=0, password=None, socket_timeout=None,
                 charset='utf-8', errors='strict'):
        """
        Create the redis protocol instance
        :param host: ip or hostname of the redis server
        :type host: str
        :param port: tcp port the redis server is listening
        :type port: int
        :param db: database
        :type db: int
        :param password: redis password
        :type password: str
        :param socket_timeout: socket timeout parameter 
            `socket.socket.settimeout`
        :type socket_timeout: int
        :param charset: charset to use to encode strings through the socket
        :type charset: str
        :param errors: encoding error level
        :type errors: str
        """
        kwargs = {
            'db': db,
            'password': password,
            'socket_timeout': socket_timeout,
            'encoding': charset,
            'encoding_errors': errors,
            'connection_class': Connection,
            'host': host,
            'port': port
            }
        return super(Redis,self).__init__( errors=errors,
                     connection_pool=connection.ConnectionPool(**kwargs))

