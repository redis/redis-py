import unittest

from tests.server_commands import ServerCommandsTestCase
from tests.connection_pool import ConnectionPoolTestCase
from tests.pipeline import PipelineTestCase
from tests.lock import LockTestCase
from tests.pubsub import PubSubTestCase, PubSubRedisDownTestCase
from tests.encoding import (PythonParserEncodingTestCase,
                            HiredisEncodingTestCase)

try:
    import hiredis
    use_hiredis = True
except ImportError:
    use_hiredis = False


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
