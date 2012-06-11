import unittest
from server_commands import ServerCommandsTestCase
from connection_pool import ConnectionPoolTestCase
from pipeline import PipelineTestCase
from lock import LockTestCase
from pubsub import PubSubTestCase, PubSubRedisDownTestCase
from encoding import PythonParserEncodingTestCase, HiredisEncodingTestCase

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
    suite.addTest(unittest.makeSuite(PubSubRedisDownTestCase))
    suite.addTest(unittest.makeSuite(PythonParserEncodingTestCase))
    if use_hiredis:
        suite.addTest(unittest.makeSuite(HiredisEncodingTestCase))
    return suite
