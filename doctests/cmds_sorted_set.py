# EXAMPLE: cmds_sorted_set
# HIDE_START
import redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
# HIDE_END

# STEP_START zadd
res = r.zadd("myzset", {"one": 1})
print(res)
# >>> 1
# REMOVE_START
assert res == 1
# REMOVE_END

res = r.zadd("myzset", {"uno": 1})
print(res)
# >>> 1
# REMOVE_START
assert res == 1
# REMOVE_END

res = r.zadd("myzset", {"two": 2, "three": 3})
print(res)
# >>> 2
# REMOVE_START
assert res == 2
# REMOVE_END

res = r.zrange("myzset", 0, -1, withscores=True)
# >>> [('one', 1.0), ('uno', 1.0), ('two', 2.0), ('three', 3.0)]
# REMOVE_START
assert res == [('one', 1.0), ('uno', 1.0), ('two', 2.0), ('three', 3.0)]
# REMOVE_END

# REMOVE_START
r.delete("myzset")
# REMOVE_END
# STEP_END

# STEP_START zrange1
res = r.zadd("myzset", {"one": 1, "two":2, "three":3})
print(res)
# >>> 3

res = r.zrange("myzset", 0, -1)
print(res)
# >>> ['one', 'two', 'three']
# REMOVE_START
assert res == ['one', 'two', 'three']
# REMOVE_END

res = r.zrange("myzset", 2, 3)
print(res)
# >>> ['three']
# REMOVE_START
assert res == ['three']
# REMOVE_END

res = r.zrange("myzset", -2, -1)
print(res)
# >>> ['two', 'three']
# REMOVE_START
assert res == ['two', 'three']
r.delete("myzset")
# REMOVE_END
# STEP_END

# STEP_START zrange2
res = r.zadd("myzset", {"one": 1, "two":2, "three":3})
res = r.zrange("myzset", 0, 1, withscores=True)
print(res)
# >>> [('one', 1.0), ('two', 2.0)]
# REMOVE_START
assert res == [('one', 1.0), ('two', 2.0)]
r.delete("myzset")
# REMOVE_END
# STEP_END

# STEP_START zrange3
res = r.zadd("myzset", {"one": 1, "two":2, "three":3})
res = r.zrange("myzset", 2, 3, byscore=True, offset=1, num=1)
print(res)
# >>> ['three']
# REMOVE_START
assert res == ['three']
r.delete("myzset")
# REMOVE_END
# STEP_END
