# EXAMPLE: cms_tutorial
# HIDE_START
"""
Code samples for Count-min sketch doc pages:
    https://redis.io/docs/latest/develop/data-types/probabilistic/count-min-sketch/
"""
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END
# REMOVE_START
r.delete("bikes:profit")
# REMOVE_END

# STEP_START cms
res1 = r.cms().initbyprob("bikes:profit", 0.001, 0.002)
print(res1)  # >>> True

res2 = r.cms().incrby("bikes:profit", ["Smoky Mountain Striker"], [100])
print(res2)  # >>> [100]

res3 = r.cms().incrby(
    "bikes:profit", ["Rocky Mountain Racer", "Cloudy City Cruiser"], [200, 150]
)
print(res3)  # >>> [200, 150]

res4 = r.cms().query("bikes:profit", "Smoky Mountain Striker")
print(res4)  # >>> [100]

res5 = r.cms().info("bikes:profit")
print(res5.width, res5.depth, res5.count)  # >>> 2000 9 450
# STEP_END
