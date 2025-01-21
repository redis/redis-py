# EXAMPLE: cmds_hash
# HIDE_START
import redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
# HIDE_END

# STEP_START hset
res1 = r.hset("myhash", "field1", "Hello")
print(res1)
# >>> 1

res2 = r.hget("myhash", "field1")
print(res2)
# >>> Hello

res3 = r.hset("myhash", mapping={"field2": "Hi", "field3": "World"})
print(res3)
# >>> 2

res4 = r.hget("myhash", "field2")
print(res4)
# >>> Hi

res5 = r.hget("myhash", "field3")
print(res5)
# >>> World

res6 = r.hgetall("myhash")
print(res6)
# >>> { "field1": "Hello", "field2": "Hi", "field3": "World" }

# REMOVE_START
assert res1 == 1
assert res2 == "Hello"
assert res3 == 2
assert res4 == "Hi"
assert res5 == "World"
assert res6 == { "field1": "Hello", "field2": "Hi", "field3": "World" }
r.delete("myhash")
# REMOVE_END
# STEP_END

# STEP_START hget
res7 = r.hset("myhash", "field1", "foo")
print(res7)
# >>> 1

res8 = r.hget("myhash", "field1")
print(res8)
# >>> foo

res9 = r.hget("myhash", "field2")
print(res9)
# >>> None

# REMOVE_START
assert res7 == 1
assert res8 == "foo"
assert res9 == None
r.delete("myhash")
# REMOVE_END
# STEP_END

# STEP_START hgetall
res10 = r.hset("myhash", mapping={"field1": "Hello", "field2": "World"})

res11 = r.hgetall("myhash")
print(res11) # >>> { "field1": "Hello", "field2": "World" }

# REMOVE_START
assert res11 == { "field1": "Hello", "field2": "World" }
r.delete("myhash")
# REMOVE_END
# STEP_END

# STEP_START hvals
res10 = r.hset("myhash", mapping={"field1": "Hello", "field2": "World"})

res11 = r.hvals("myhash")
print(res11) # >>> [ "Hello", "World" ]

# REMOVE_START
assert res11 == [ "Hello", "World" ]
r.delete("myhash")
# REMOVE_END
# STEP_END