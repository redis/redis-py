import unittest
from server_commands import ServerCommandsTestCase
from connection_pool import ConnectionPoolTestCase
from pipeline import PipelineTestCase

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ServerCommandsTestCase))
    suite.addTest(unittest.makeSuite(ConnectionPoolTestCase))
    suite.addTest(unittest.makeSuite(PipelineTestCase))
    return suite
