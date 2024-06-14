# EXAMPLE: bf_tutorial
# HIDE_START
"""
Code samples for Bloom filter doc pages:
    https://redis.io/docs/latest/develop/data-types/probabilistic/bloom-filter/
"""
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# STEP_START bloom
res1 = r.bf().reserve("bikes:models", 0.01, 1000)
print(res1)  # >>> True

res2 = r.bf().add("bikes:models", "Smoky Mountain Striker")
print(res2)  # >>> True

res3 = r.bf().exists("bikes:models", "Smoky Mountain Striker")
print(res3)  # >>> True

res4 = r.bf().madd(
    "bikes:models",
    "Rocky Mountain Racer",
    "Cloudy City Cruiser",
    "Windy City Wippet",
)
print(res4)  # >>> [True, True, True]

res5 = r.bf().mexists(
    "bikes:models",
    "Rocky Mountain Racer",
    "Cloudy City Cruiser",
    "Windy City Wippet",
)
print(res5)  # >>> [True, True, True]
# STEP_END

# REMOVE_START
assert res1 is True
# REMOVE_END
