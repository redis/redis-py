import unittest
from server_commands import ServerCommands

def all_tests():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ServerCommands))
    return suite
