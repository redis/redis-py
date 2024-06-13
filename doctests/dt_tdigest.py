# EXAMPLE: tdigest_tutorial
# HIDE_START
"""
Code samples for t-digest pages:
    https://redis.io/docs/latest/develop/data-types/probabilistic/t-digest/
"""

import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# REMOVE_START
r.delete("racer_ages")
r.delete("bikes:sales")
# REMOVE_END

# STEP_START tdig_start
res1 = r.tdigest().create("bikes:sales", 100)
print(res1)  # >>> True

res2 = r.tdigest().add("bikes:sales", [21])
print(res2)  # >>> OK

res3 = r.tdigest().add("bikes:sales", [150, 95, 75, 34])
print(res3)  # >>> OK
# STEP_END

# REMOVE_START
assert res1 is True
# REMOVE_END

# STEP_START tdig_cdf
res4 = r.tdigest().create("racer_ages")
print(res4)  # >>> True

res5 = r.tdigest().add(
    "racer_ages",
    [
        45.88,
        44.2,
        58.03,
        19.76,
        39.84,
        69.28,
        50.97,
        25.41,
        19.27,
        85.71,
        42.63,
    ],
)
print(res5)  # >>> OK

res6 = r.tdigest().rank("racer_ages", 50)
print(res6)  # >>> [7]

res7 = r.tdigest().rank("racer_ages", 50, 40)
print(res7)  # >>> [7, 4]
# STEP_END

# STEP_START tdig_quant
res8 = r.tdigest().quantile("racer_ages", 0.5)
print(res8)  # >>> [44.2]

res9 = r.tdigest().byrank("racer_ages", 4)
print(res9)  # >>> [42.63]
# STEP_END

# STEP_START tdig_min
res10 = r.tdigest().min("racer_ages")
print(res10)  # >>> 19.27

res11 = r.tdigest().max("racer_ages")
print(res11)  # >>> 85.71
# STEP_END

# STEP_START tdig_reset
res12 = r.tdigest().reset("racer_ages")
print(res12)  # >>> OK
# STEP_END
