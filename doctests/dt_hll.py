# # EXAMPLE: hll_tutorial
# HIDE_START
"""
Code samples for HyperLogLog doc pages:
    https://redis.io/docs/latest/develop/data-types/probabilistic/hyperloglogs/
"""

import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# REMOVE_START
r.delete("bikes", "commuter_bikes", "all_bikes")
# REMOVE_END

# STEP_START pfadd
res1 = r.pfadd("bikes", "Hyperion", "Deimos", "Phoebe", "Quaoar")
print(res1)  # >>> 1

res2 = r.pfcount("bikes")
print(res2)  # >>> 4

res3 = r.pfadd("commuter_bikes", "Salacia", "Mimas", "Quaoar")
print(res3)  # >>> 1

res4 = r.pfmerge("all_bikes", "bikes", "commuter_bikes")
print(res4)  # >>> True

res5 = r.pfcount("all_bikes")
print(res5)  # >>> 6
# STEP_END

# REMOVE_START
assert res4 is True
# REMOVE_END
