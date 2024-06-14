# EXAMPLE: cuckoo_tutorial
# HIDE_START
"""
Code samples for Cuckoo filter doc pages:
    https://redis.io/docs/latest/develop/data-types/probabilistic/cuckoo-filter/
"""
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# REMOVE_START
r.delete("bikes:models")
# REMOVE_END

# STEP_START cuckoo
res1 = r.cf().reserve("bikes:models", 1000000)
print(res1)  # >>> True

res2 = r.cf().add("bikes:models", "Smoky Mountain Striker")
print(res2)  # >>> 1

res3 = r.cf().exists("bikes:models", "Smoky Mountain Striker")
print(res3)  # >>> 1

res4 = r.cf().exists("bikes:models", "Terrible Bike Name")
print(res4)  # >>> 0

res5 = r.cf().delete("bikes:models", "Smoky Mountain Striker")
print(res5)  # >>> 1
# STEP_END

# REMOVE_START
assert res1 is True
assert res5 == 1
# REMOVE_END
