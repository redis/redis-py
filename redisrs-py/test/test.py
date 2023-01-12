import redis

import redisrs_py
import hiredis
r = redis.from_url('redis://localhost:12005')
print(r.ping())


# print(r.hset("foo1", "42", 4.09,))
# print(r.hset("foo2", items=[42, "zoo", 'a', 67, 42, 3.56]))
print(r.hset("foo3", mapping={42: "zoo", 'a': 67, 43: 3.56}))

# print(redisrs_py.pack_command(('ping',)))

# print(redisrs_py.pack_command((b'HSET', 'foo3', 42, 'zoo', 'a', 67, 43, 3.56)))

# args = (b'HSET', 'foo3', 42, 'zoo', 'a', 67, 43, 3.56)
# #args = ['bbb']
# print(hiredis.pack_command(args))
# bytes_cmd = b'hset key1 foo bar zoo 4567890 baz 9009870987098  zoo 3.14569808'
# print(hiredis.pack_bytes(bytes_cmd))

import unittest

class TestPackBytes(unittest.TestCase):
    def test_basic(self):
        cmd = b'HSET foo3 42 zoo a 67 43 3.56'
        expected_packed = b'*8\r\n$4\r\nHSET\r\n$4\r\nfoo3\r\n$2\r\n42\r\n$3\r\nzoo\r\n$1\r\na\r\n$2\r\n67\r\n$2\r\n43\r\n$4\r\n3.56\r\n'
        packed_cmd = hiredis.pack_bytes(cmd)
        self.assertEqual(packed_cmd, expected_packed)
        packed_cmd = redisrs_py.pack_bytes(cmd)
        self.assertEqual(packed_cmd, expected_packed)

    def test_multiple_spaces(self):
        cmd = b'            HSET    foo3 42 zoo a    67 43 3.56        '
        expected_packed = b'*8\r\n$4\r\nHSET\r\n$4\r\nfoo3\r\n$2\r\n42\r\n$3\r\nzoo\r\n$1\r\na\r\n$2\r\n67\r\n$2\r\n43\r\n$4\r\n3.56\r\n'
        packed_cmd = hiredis.pack_bytes(cmd)
        self.assertEqual(packed_cmd, expected_packed)
        packed_cmd = redisrs_py.pack_bytes(cmd)
        self.assertEqual(packed_cmd, expected_packed)

if __name__ == '__main__':
    unittest.main()
