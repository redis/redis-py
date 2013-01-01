#coding:utf-8
""" Note: Setup an other redis instance at port 9682 to run this test """

import unittest
import redis


class MirrorServerTestCase(unittest.TestCase):

    def setUp(self):
        self.client = redis.Redis(host='localhost', db=9,
                                  mirror='localhost:9682')

    def tearDown(self):
        self.client.flushdb()
        self.client.connection_pool.disconnect()

    def test_mirror_server(self):
        c = self.client
        c.set('a', 'b')
        self.assertEqual(c.primary_inst['a'], c.mirror_inst['a'])
