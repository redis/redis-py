import os
import subprocess
import sys

from redis import Redis, ConnectionError


class ImproperlyConfigured(Exception):
    pass


class RedisServer(object):
    """
    Context manager to start and stop a redis server if a path to
    a redis executable is provided.  Optionally a configuration file
    can be specified.

    No-ops if the executable path is not provided and assumes a redis
    server is already running.  In such case, certain tests will be
    skipped, since they need to be able to shutdown/restart the server
    in order to pass.
    """
    def __init__(self, server_path=None, conf=None, verbosity=0):
        self.configure(server_path, conf, verbosity)
        self.process = None

    def configure(self, server_path=None, conf=None, verbosity=0):
        if server_path is not None:
            server_path = os.path.abspath(server_path)
        self.server_path = server_path

        if conf is not None:
            conf = os.path.abspath(conf)
        self.conf = conf

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

        if self.verbosity == 0:
            stream = open(os.devnull, 'wb')
        else:
            stream = sys.stdout

        self.process = subprocess.Popen(args, stdout=stream, stderr=stream)

        # Poll the redis server until it is accepting connections
        redis = Redis()
        while True:
            try:
                if redis.ping():
                    break
            except ConnectionError:
                pass
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.stop()

    def stop(self):
        if self.process is not None:
            self.process.kill()
            self.process = None


server = RedisServer()