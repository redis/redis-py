# EXAMPLE: bitmap_tutorial
# HIDE_START
"""
Code samples for Bitmap doc pages:
    https://redis.io/docs/latest/develop/data-types/bitmaps/
"""
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# REMOVE_START
r.delete("pings:2024-01-01-00:00")
# REMOVE_END

# STEP_START ping
res1 = r.setbit("pings:2024-01-01-00:00", 123, 1)
print(res1)  # >>> 0

res2 = r.getbit("pings:2024-01-01-00:00", 123)
print(res2)  # >>> 1

res3 = r.getbit("pings:2024-01-01-00:00", 456)
print(res3)  # >>> 0
# STEP_END

# REMOVE_START
assert res1 == 0
# REMOVE_END

# STEP_START bitcount
# HIDE_START
r.setbit("pings:2024-01-01-00:00", 123, 1)
# HIDE_END
res4 = r.bitcount("pings:2024-01-01-00:00")
print(res4)  # >>> 1
# STEP_END
# REMOVE_START
assert res4 == 1
# REMOVE_END
