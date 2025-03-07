# EXAMPLE: cmds_set
# HIDE_START
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# STEP_START sadd
res1 = r.sadd("myset", "Hello", "World")
print(res1)  # >>> 2

res2 = r.sadd("myset", "World")
print(res2)  # >>> 0

res3 = r.smembers("myset")
print(res3)  # >>> {'Hello', 'World'}

# REMOVE_START
assert res3 == {'Hello', 'World'}
r.delete('myset')
# REMOVE_END
# STEP_END

# STEP_START smembers
res4 = r.sadd("myset", "Hello", "World")
print(res4)  # >>> 2

res5 = r.smembers("myset")
print(res5)  # >>> {'Hello', 'World'}

# REMOVE_START
assert res5 == {'Hello', 'World'}
r.delete('myset')
# REMOVE_END
# STEP_END