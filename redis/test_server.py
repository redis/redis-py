import re
import tempfile
import os
import time
import logging
import subprocess

from redis import utils


logger = logging.getLogger(__name__)

class KeyPairMapping(dict):
    "support class to map text <-> dict"
    def __init__(self, text=None):
        if not text:
            text = self.template

        super(KeyPairMapping, self).__init__()
        for line in text.split("\n"):
            line = line.strip()
            if line.startswith("#") or line == "": continue
            i = line.find(" ")
            if i == -1:
                key = line.strip()
                value = ""
            else:
                key = line[:i]
                value = line[i+1:].strip()
            self[key] = value
        
    @staticmethod
    def to_str(data, eol='\n'):
        result = []
        for k in sorted(data.keys()):
            if k.startswith("__"): continue
            result.append(str(k) + " " + str(data[k]))
        if eol:
            result = eol.join(result)
        return result


class ServerConfig(KeyPairMapping):
    template = """
# Skeleton template for config a redis server
daemonize       no
port            6379
bind            127.0.0.1
timeout         0
tcp-keepalive   0
loglevel        notice
databases       16
"""



class TestServerBase(object):
    """test server launcher class

    This class wraps a redis-server instance providing support for 
    testing.

"""
    def __init__(self, keypairs=None):

        self.server_config = ServerConfig()
        self.server_config.update(keypairs if keypairs else {})

        self.config = {}
        self.config['redis'] = "redis-server"
        self.config['startup_delay_s'] = 2
        self.__tmpfiles = []
        self.__server = None


    def _tmpfile(self, ghost=False):
        # We handle the allocated tmp files
        fd, fname = tempfile.mkstemp()
        os.close(fd)
        if ghost:
            os.unlink(fname)
        self.__tmpfiles.append(fname) 
        return fname

    def _cleanup(self):
        for fname in [ n for n in self.__tmpfiles if os.path.exists(n) ]:
            logger.debug("removing temp file %s" % fname)
            os.unlink(fname)

    def __enter__(self):
        return self

    def __exit__(self, tb_type, tb_value, tb_object):
        self.stop()

    def get_pool_args(self):
        return { 'host' : self.server_config['bind'], 'port' : int(self.server_config['port']), 'db' : 0 }

    def start(self, keypairs=None):

        self.server_config.update(keypairs if keypairs else {})

        # we need those defined
        missing = set(['dir', 'dbfilename', 'pidfile', 'logfile',]).difference(self.server_config.keys())
        if missing:
            raise ValueError("missing the following keys from config: '%s'" % (", ".join(missing)))

        # the full qualified pathname to the redis server
        server_exe = utils.which(self.config['redis'])
        logger.debug("redis-server exe from: %s" % server_exe)

        # creating missing dirs
        dir_names = [ self.server_config['dir'], ]
        for key in [ 'pidfile', 'logfile', ]:
            dir_names.append(os.path.dirname(self.server_config[key]))
        for dir_name in dir_names:
            if not os.path.exists(dir_name):
                logger.debug("creating dir: %s" % dir_name)
                os.makedirs(dir_name)
        for n in [ 'dir', 'dbfilename', 'pidfile', 'logfile', ]:
            logger.debug("using %s: %s" % (n, self.server_config[n]))

        # the main redis config file (generated on the fly from the server_config dict)
        config_file = self._tmpfile()
        fp = open(config_file, "w")
        fp.write(KeyPairMapping.to_str(self.server_config))
        fp.flush
        fp.close()
        logger.debug("temp config file: %s" % config_file)


        # Launching the server
        args = [ server_exe, config_file, ]
        logger.debug("redis server launch command: %s" % ' '.join(args))

        self.__server = subprocess.Popen(args)
        logger.debug("redis server started with pid %i" % self.__server.pid)

        if 'startup_delay_s' in self.config:
            logger.debug("waiting %is before listening" % self.config['startup_delay_s'])
            time.sleep(self.config['startup_delay_s'])
        logger.debug(str(self.__class__.__name__) + " listening at %(bind)s:%(port)s" % self.server_config)

    def stop(self):
        if not self.__server:
            return
        self.__server.terminate()
        self.__server.wait()
        self._cleanup()
        self.__server = None


class TestServer(TestServerBase):
    def start(self, keypairs=None):
        newdata = keypairs.copy() if keypairs else {}

        if not 'pidfile' in newdata and not 'pidfile' in self.server_config:
            newdata['pidfile'] = self._tmpfile()
        if not 'logfile' in newdata and not 'logfile' in self.server_config:
            newdata['logfile'] = self._tmpfile()

        full_db_path = self._tmpfile(ghost=True)
        newdata['dir'] = os.path.dirname(full_db_path)
        newdata['dbfilename'] = os.path.basename(full_db_path)

        return super(TestServer, self).start(newdata)
