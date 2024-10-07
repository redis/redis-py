# EXAMPLE: sets_tutorial
# HIDE_START
"""
Code samples for Set doc pages:
    https://redis.io/docs/latest/develop/data-types/sets/
"""

import redis

r = redis.Redis(decode_responses=True)
# HIDE_END
# REMOVE_START
r.delete("bikes:racing:france")
r.delete("bikes:racing:usa")
# REMOVE_END

# STEP_START sadd
res1 = r.sadd("bikes:racing:france", "bike:1")
print(res1)  # >>> 1

res2 = r.sadd("bikes:racing:france", "bike:1")
print(res2)  # >>> 0

res3 = r.sadd("bikes:racing:france", "bike:2", "bike:3")
print(res3)  # >>> 2

res4 = r.sadd("bikes:racing:usa", "bike:1", "bike:4")
print(res4)  # >>> 2
# STEP_END

# REMOVE_START
assert res1 == 1
assert res2 == 0
assert res3 == 2
assert res4 == 2
# REMOVE_END

# STEP_START sismember
# HIDE_START
r.sadd("bikes:racing:france", "bike:1", "bike:2", "bike:3")
r.sadd("bikes:racing:usa", "bike:1", "bike:4")
# HIDE_END
res5 = r.sismember("bikes:racing:usa", "bike:1")
print(res5)  # >>> 1

res6 = r.sismember("bikes:racing:usa", "bike:2")
print(res6)  # >>> 0
# STEP_END

# REMOVE_START
assert res5 == 1
assert res6 == 0
# REMOVE_END

# STEP_START sinter
# HIDE_START
r.sadd("bikes:racing:france", "bike:1", "bike:2", "bike:3")
r.sadd("bikes:racing:usa", "bike:1", "bike:4")
# HIDE_END
res7 = r.sinter("bikes:racing:france", "bikes:racing:usa")
print(res7)  # >>> {'bike:1'}
# STEP_END

# REMOVE_START
assert res7 == {"bike:1"}
# REMOVE_END

# STEP_START scard
# HIDE_START
r.sadd("bikes:racing:france", "bike:1", "bike:2", "bike:3")
# HIDE_END
res8 = r.scard("bikes:racing:france")
print(res8)  # >>> 3
# STEP_END

# REMOVE_START
assert res8 == 3
r.delete("bikes:racing:france")
# REMOVE_END

# STEP_START sadd_smembers
res9 = r.sadd("bikes:racing:france", "bike:1", "bike:2", "bike:3")
print(res9)  # >>> 3

res10 = r.smembers("bikes:racing:france")
print(res10)  # >>> {'bike:1', 'bike:2', 'bike:3'}
# STEP_END

# REMOVE_START
assert res9 == 3
assert res10 == {'bike:1', 'bike:2', 'bike:3'}
# REMOVE_END

# STEP_START smismember
res11 = r.sismember("bikes:racing:france", "bike:1")
print(res11)  # >>> 1

res12 = r.smismember("bikes:racing:france", "bike:2", "bike:3", "bike:4")
print(res12)  # >>> [1, 1, 0]
# STEP_END

# REMOVE_START
assert res11 == 1
assert res12 == [1, 1, 0]
# REMOVE_END

# STEP_START sdiff
r.sadd("bikes:racing:france", "bike:1", "bike:2", "bike:3")
r.sadd("bikes:racing:usa", "bike:1", "bike:4")

res13 = r.sdiff("bikes:racing:france", "bikes:racing:usa")
print(res13)  # >>> {'bike:2', 'bike:3'}
# STEP_END

# REMOVE_START
assert res13 == {'bike:2', 'bike:3'}
r.delete("bikes:racing:france")
r.delete("bikes:racing:usa")
# REMOVE_END

# STEP_START multisets
r.sadd("bikes:racing:france", "bike:1", "bike:2", "bike:3")
r.sadd("bikes:racing:usa", "bike:1", "bike:4")
r.sadd("bikes:racing:italy", "bike:1", "bike:2", "bike:3", "bike:4")

res13 = r.sinter("bikes:racing:france", "bikes:racing:usa", "bikes:racing:italy")
print(res13)  # >>> {'bike:1'}

res14 = r.sunion("bikes:racing:france", "bikes:racing:usa", "bikes:racing:italy")
print(res14)  # >>> {'bike:1', 'bike:2', 'bike:3', 'bike:4'}

res15 = r.sdiff("bikes:racing:france", "bikes:racing:usa", "bikes:racing:italy")
print(res15)  # >>> {}

res16 = r.sdiff("bikes:racing:usa", "bikes:racing:france")
print(res16)  # >>> {'bike:4'}

res17 = r.sdiff("bikes:racing:france", "bikes:racing:usa")
print(res17)  # >>> {'bike:2', 'bike:3'}
# STEP_END

# REMOVE_START
assert res13 == {'bike:1'}
assert res14 == {'bike:1', 'bike:2', 'bike:3', 'bike:4'}
assert res15 == {}
assert res16 == {'bike:4'}
assert res17 == {'bike:2', 'bike:3'}
r.delete("bikes:racing:france")
r.delete("bikes:racing:usa")
r.delete("bikes:racing:italy")
# REMOVE_END

# STEP_START srem
r.sadd("bikes:racing:france", "bike:1", "bike:2", "bike:3", "bike:4", "bike:5")

res18 = r.srem("bikes:racing:france", "bike:1")
print(res18)  # >>> 1

res19 = r.spop("bikes:racing:france")
print(res19)  # >>> bike:3

res20 = r.smembers("bikes:racing:france")
print(res20)  # >>> {'bike:2', 'bike:4', 'bike:5'}

res21 = r.srandmember("bikes:racing:france")
print(res21)  # >>> bike:4
# STEP_END

# REMOVE_START
assert res18 == 1
# none of the other results are deterministic
# REMOVE_END
