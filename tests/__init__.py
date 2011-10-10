import unittest
from tests.server_commands import ServerCommandsTestCase
from tests.connection_pool import ConnectionPoolTestCase
from tests.pipeline import PipelineTestCase
from tests.lock import LockTestCase
from tests.pubsub import PubSubTestCase

use_hiredis = False
try:
    import hiredis
    use_hiredis = True
except ImportError:
    pass

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ServerCommandsTestCase))
    suite.addTest(unittest.makeSuite(ConnectionPoolTestCase))
    suite.addTest(unittest.makeSuite(PipelineTestCase))
    suite.addTest(unittest.makeSuite(LockTestCase))
    suite.addTest(unittest.makeSuite(PubSubTestCase))
    return suite
