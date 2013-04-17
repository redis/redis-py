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


import redis
import redis.test_server


FREEPORT = os.getenv("FREEPORT", 9999)


class SimpleTestServerCase(unittest.TestCase):
    
    def test_check_failed_redis_server(self):
        "chek for a non-existent redis server executable"
        server = redis.test_server.TestServer()
        server.config['redis'] = 'bahse0edjwicwi'
        self.assertRaises(IOError, server.start)

    @unittest.skipIf(sys.version_info[:2] < (2, 6), "python < 2.6 doesn't support context managers")
    def test_startup_server_context(self):
        "launches the redis server with a context manager"
        with redis.test_server.TestServer({ 'port' : FREEPORT, }) as server:
            server.config['startup_delay_s'] = 3
            server.start()
            
            pool = redis.ConnectionPool(**server.get_pool_args())
            connection = redis.Redis(connection_pool=pool)
            cfg = connection.config_get()
            self.assertEquals(cfg['bind'], '127.0.0.1')
            self.assertEquals(cfg['port'], str(FREEPORT))

    def test_startup_server(self):
        "launches the redis server in a ordinary way"
        server = redis.test_server.TestServer({ 'port' : FREEPORT, })
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
    unittest.main()



