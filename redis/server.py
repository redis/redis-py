import os
import re
import subprocess
import sys
from threading import Timer

from redis import Redis, ConnectionError


class ImproperlyConfigured(Exception):
    pass


class RedisServer(object):
    """
    Context manager to start and stop a redis server if a path to
    a redis executable is provided.  Optionally a configuration file
    can be specified.

    No-ops if the executable path is not provided and assumes a redis
    server is already running.
    """
    def __init__(self, server_path=None, conf=None, options=None, verbosity=0):
        self.configure(server_path, conf, options, verbosity)
        self.process = None

    def configure(self, server_path=None, conf=None, options=None, verbosity=0):
        if server_path is not None:
            server_path = os.path.abspath(server_path)
        self.server_path = server_path

        if conf is not None:
            conf = os.path.abspath(conf)
        self.conf = conf

        if options is None:
            options = {}
        self.options = options or {}

        self.verbosity = verbosity

    def __enter__(self):
        self.start()

    def start(self):
        # short-circuit this and assume a redis server is running
        if self.server_path is None:
            return

        if not os.path.exists(self.server_path):
            raise ImproperlyConfigured("Path to redis-server executable does not exist")

        if self.conf is not None and not os.path.exists(self.conf):
            raise ImproperlyConfigured("Path to redis configuration file does not exist")

        args = [self.server_path]

        if self.conf:
            args.append(self.conf)

        for option, value in self.options.items():
            args.extend(["--%s" % option, value])

        if self.verbosity == 0:
            stream = open(os.devnull, 'wb')
        else:
            stream = sys.stdout

        self.process = subprocess.Popen(args, stdout=stream, stderr=stream)

        # Poll the redis server until it is accepting connections
        redis = Redis(host=self.host, port=self.port, password=self.password)

        # Create a threading.Timer to timeout the while loop so it
        # doesn't infinitely loop.
        self.timed_out = False
        t = Timer(2.0, self.timeout)
        t.start()

        while not self.timed_out:
            try:
                if redis.ping():
                    break
            except ConnectionError, e:
                self.last_exception = e

        if self.timed_out:
            raise self.last_exception
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.stop()

    def stop(self):
        if self.process is not None:
            self.process.kill()
            self.process = None

    def timeout(self):
        self.timed_out = True

    def get_configuration(self):
        # set defaults
        self._host = '127.0.0.1'
        self._port = 6379
        self._password = None

        # short-circuit if there is no conf, just use the defaults
        if self.conf is None or not os.path.exists(self.conf):
            return

        # Open the redis conf and search for the port and bind options
        with open(self.conf) as f:
            contents = f.read()
            for line in contents.splitlines():
                line = line.strip()
                if line.startswith('port'):
                    self._port = int(re.split(r"[ ]+", line)[1])
                if line.startswith('bind'):
                    self._host = re.split(r"[ ]+", line)[1]
                if line.startswith('requirepass'):
                    self._password = re.split(r'[ ]+', line)[1]

    @property
    def password(self):
        if hasattr(self, '_password'):
            return self._password
        self.get_configuration()
        return self._password

    @property
    def host(self):
        if hasattr(self, '_host'):
            return self._host
        self.get_configuration()
        return self._host

    @property
    def port(self):
        if hasattr(self, '_port'):
            return self._port
        self.get_configuration()
        return self._port

server = RedisServer
