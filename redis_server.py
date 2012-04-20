import os
import subprocess

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
    def __init__(self, path=None, conf=None):
        self.configure(path, conf)
        self.process = None

    def configure(self, path, conf):
        if path is not None:
            path = os.path.abspath(path)
        self.path = path

        if conf is not None:
            conf = os.path.abspath(conf)
        self.conf = conf

    def __enter__(self):
        self.start()

    def start(self):
        # short-circuit this and assume a redis server is running
        if self.path is None:
            return

        if not os.path.exists(self.path):
            raise ImproperlyConfigured("Path to redis file does not exist")

        if self.conf is not None and not os.path.exists(self.conf):
            raise ImproperlyConfigured("Path to redis configuration file does not exist")

        args = [self.path]
        if self.conf:
            args.append(self.conf)

        devnull = open(os.devnull, 'wb')
        self.process = subprocess.Popen(args, stdout=devnull, stderr=devnull)

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