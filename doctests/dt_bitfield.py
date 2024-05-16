# EXAMPLE: bitfield_tutorial
# HIDE_START
"""
Code samples for Bitfield doc pages:
    https://redis.io/docs/latest/develop/data-types/bitfields/
"""
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# REMOVE_START
r.delete("bike:1:stats")
# REMOVE_END

# STEP_START bf
bf = r.bitfield("bike:1:stats")
res1 = bf.set("u32", "#0", 1000).execute()
print(res1)  # >>> [0]

res2 = bf.incrby("u32", "#0", -50).incrby("u32", "#1", 1).execute()
print(res2)  # >>> [950, 1]

res3 = bf.incrby("u32", "#0", 500).incrby("u32", "#1", 1).execute()
print(res3)  # >>> [1450, 2]

res4 = bf.get("u32", "#0").get("u32", "#1").execute()
print(res4)  # >>> [1450, 2]
# STEP_END

# REMOVE_START
assert res1 == [0]
assert res4 == [1450, 2]
# REMOVE_END
