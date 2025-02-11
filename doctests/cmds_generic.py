# EXAMPLE: cmds_generic
# HIDE_START
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# STEP_START del
res = r.set("key1", "Hello")
print(res)
# >>> True

res = r.set("key2", "World")
print(res)
# >>> True

res = r.delete("key1", "key2", "key3")
print(res)
# >>> 2
# REMOVE_START
assert res == 2
# REMOVE_END
# STEP_END

# STEP_START expire
res = r.set("mykey", "Hello")
print(res)
# >>> True

res = r.expire("mykey", 10)
print(res)
# >>> True

res = r.ttl("mykey")
print(res)
# >>> 10
# REMOVE_START
assert res == 10
# REMOVE_END

res = r.set("mykey", "Hello World")
print(res)
# >>> True

res = r.ttl("mykey")
print(res)
# >>> -1
# REMOVE_START
assert res == -1
# REMOVE_END

res = r.expire("mykey", 10, xx=True)
print(res)
# >>> False
# REMOVE_START
assert res == False
# REMOVE_END

res = r.ttl("mykey")
print(res)
# >>> -1
# REMOVE_START
assert res == -1
# REMOVE_END

res = r.expire("mykey", 10, nx=True)
print(res)
# >>> True
# REMOVE_START
assert res == True
# REMOVE_END

res = r.ttl("mykey")
print(res)
# >>> 10
# REMOVE_START
assert res == 10
r.delete("mykey")
# REMOVE_END
# STEP_END

# STEP_START ttl
res = r.set("mykey", "Hello")
print(res)
# >>> True

res = r.expire("mykey", 10)
print(res)
# >>> True

res = r.ttl("mykey")
print(res)
# >>> 10
# REMOVE_START
assert res == 10
r.delete("mykey")
# REMOVE_END
# STEP_END

# STEP_START scan1
res = r.sadd("myset", *set([1, 2, 3, "foo", "foobar", "feelsgood"]))
print(res)
# >>> 6

res = list(r.sscan_iter("myset", match="f*"))
print(res)
# >>> ['foobar', 'foo', 'feelsgood']
# REMOVE_START
assert sorted(res) == sorted(["foo", "foobar", "feelsgood"])
r.delete("myset")
# REMOVE_END
# STEP_END

# STEP_START scan2
# REMOVE_START
for i in range(1, 1001):
    r.set(f"key:{i}", i)
# REMOVE_END

cursor, key = r.scan(cursor=0, match='*11*')
print(cursor, key)

cursor, key = r.scan(cursor, match='*11*')
print(cursor, key)

cursor, key = r.scan(cursor, match='*11*')
print(cursor, key)

cursor, key = r.scan(cursor, match='*11*')
print(cursor, key)

cursor, keys = r.scan(cursor, match='*11*', count=1000)
print(cursor, keys)

# REMOVE_START
assert len(keys) == 18
cursor = '0'
prefix = "key:*"
while cursor != 0:
    cursor, keys = r.scan(cursor=cursor, match=prefix, count=1000)
    if keys:
        r.delete(*keys)
# REMOVE_END
# STEP_END

# STEP_START scan3
res = r.geoadd("geokey", (0, 0, "value"))
print(res)
# >>> 1

res = r.zadd("zkey", {"value": 1000})
print(res)
# >>> 1

res = r.type("geokey")
print(res)
# >>> zset
# REMOVE_START
assert res == "zset"
# REMOVE_END

res = r.type("zkey")
print(res)
# >>> zset
# REMOVE_START
assert res == "zset"
# REMOVE_END

cursor, keys = r.scan(cursor=0, _type="zset")
print(keys)
# >>> ['zkey', 'geokey']
# REMOVE_START
assert sorted(keys) == sorted(["zkey", "geokey"])
r.delete("geokey", "zkey")
# REMOVE_END
# STEP_END

# STEP_START scan4
res = r.hset("myhash", mapping={"a": 1, "b": 2})
print(res)
# >>> 2

cursor, keys = r.hscan("myhash", 0)
print(keys)
# >>> {'a': '1', 'b': '2'}
# REMOVE_START
assert keys == {'a': '1', 'b': '2'}
# REMOVE_END

cursor, keys = r.hscan("myhash", 0, no_values=True)
print(keys)
# >>> ['a', 'b']
# REMOVE_START
assert keys == ['a', 'b']
# REMOVE_END

# REMOVE_START
r.delete("myhash")
# REMOVE_END
# STEP_END
