import os
import unittest

import redis
import redis.test_server


FREEPORT = os.getenv("FREEPORT", 6379)


class SimpleTestServerCase(unittest.TestCase):
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



