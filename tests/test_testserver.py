# compat layer for python < 2.6 ...
# this is to prevent the with statement to generate a syntax error
from __future__ import with_statement
# we need skipIf from unittest (standard from 2.7)
import unittest
if not hasattr(unittest, "skipIf"):
    import unittest2 as unittest
# end of compat layer

import sys
import os
import logging

import redis
import redis.test_server


FREEPORT = os.getenv("FREEPORT", 9999)


class SimpleTestServerKeyMapTest(unittest.TestCase):
    def __init__(self, *argv, **kwargs):
        super(SimpleTestServerKeyMapTest, self).__init__(*argv, **kwargs)

        # This is to show the whole difference
        self.maxDiff = None
        # This is to fix the difference using the difflib
        self.addTypeEqualityFunc(str, self.assertMultiLineEqual)
        self.addTypeEqualityFunc(unicode, self.assertMultiLineEqual)

    def setUp(self):
        self.SIMPLE_REDIS_CONF = """
activerehashing yes
auto-aof-rewrite-percentage 100
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit pubsub 32mb 8mb 60
client-output-buffer-limit slave 256mb 64mb 60
dbfilename dump.rdb
dir ./
hash-max-ziplist-entries 512
list-max-ziplist-entries 512
repl-disable-tcp-nodelay no
save 300 10
save 60 10000
save 900 1
zset-max-ziplist-entries 128
zset-max-ziplist-value 64
"""
        self.conf = redis.test_server.KeyPairMapping(self.SIMPLE_REDIS_CONF)

    def test_reading_into_a_keypairmapping_class(self):
        "test verifying the correct multivalued keys load"

        result = redis.test_server.KeyPairMapping.to_str(self.conf)
        self.assertEquals(self.SIMPLE_REDIS_CONF.strip(), result)

    def test_values_in_a_keypairmapping_class(self):
        "test checking config values loaded"

        result = redis.test_server.KeyPairMapping.to_str(self.conf)
        self.assertEquals(self.conf['save'], ['300 10', '60 10000', '900 1'])
        self.assertEquals(self.conf['dir'], './')
        self.assertEquals(self.conf['zset-max-ziplist-value'], '64')


class SimpleTestServerCase(unittest.TestCase):

    def test_check_failed_redis_server(self):
        "chek for a non-existent redis server executable"
        server = redis.test_server.TestServer()
        server.config['redis'] = 'bahse0edjwicwi'
        self.assertRaises(IOError, server.start)

    @unittest.skipIf(sys.version_info[:2] < (2, 6),
                     "python < 2.6 doesn't support context managers")
    def test_startup_server_context(self):
        "launches the redis server with a context manager"
        with redis.test_server.TestServer({'port': FREEPORT}) as server:
            server.config['startup_delay_s'] = 3
            server.start()

            pool = redis.ConnectionPool(**server.get_pool_args())
            connection = redis.Redis(connection_pool=pool)
            cfg = connection.config_get()
            self.assertEquals(cfg['bind'], '127.0.0.1')
            self.assertEquals(cfg['port'], str(FREEPORT))

    def test_startup_server(self):
        "launches the redis server in a ordinary way"
        server = redis.test_server.TestServer({'port': FREEPORT})
        server.config['startup_delay_s'] = 3

        try:
            server.start()

            pool = redis.ConnectionPool(**server.get_pool_args())
            connection = redis.Redis(connection_pool=pool)

            cfg = connection.config_get()
            self.assertEquals(cfg['bind'], '127.0.0.1')
            self.assertEquals(cfg['port'], str(FREEPORT))
        finally:
            server.stop()


if __name__ == "__main__":
    logging.basicConfig()
    unittest.main()
