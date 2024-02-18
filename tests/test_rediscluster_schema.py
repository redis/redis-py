#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


import unittest

from redis import Redis
from redis.cluster import RedisCluster


class Test(unittest.TestCase):

    def test(self):
        res = Redis.from_url("rediscluster://localhost:7000")
        self.assertIsInstance(res, RedisCluster)
