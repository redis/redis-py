# EXAMPLE: hash_tutorial
# HIDE_START
"""
Code samples for Hash doc pages:
    https://redis.io/docs/latest/develop/data-types/hashes/
"""
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END
# STEP_START set_get_all
res1 = r.hset(
    "bike:1",
    mapping={
        "model": "Deimos",
        "brand": "Ergonom",
        "type": "Enduro bikes",
        "price": 4972,
    },
)
print(res1)
# >>> 4

res2 = r.hget("bike:1", "model")
print(res2)
# >>> 'Deimos'

res3 = r.hget("bike:1", "price")
print(res3)
# >>> '4972'

res4 = r.hgetall("bike:1")
print(res4)
# >>> {'model': 'Deimos', 'brand': 'Ergonom', 'type': 'Enduro bikes', 'price': '4972'}

# STEP_END

# REMOVE_START
assert res1 == 4
assert res2 == "Deimos"
assert res3 == "4972"
assert res4 == {
    "model": "Deimos",
    "brand": "Ergonom",
    "type": "Enduro bikes",
    "price": "4972",
}
# REMOVE_END

# STEP_START hmget
res5 = r.hmget("bike:1", ["model", "price"])
print(res5)
# >>> ['Deimos', '4972']
# STEP_END

# REMOVE_START
assert res5 == ["Deimos", "4972"]
# REMOVE_END

# STEP_START hincrby
res6 = r.hincrby("bike:1", "price", 100)
print(res6)
# >>> 5072
res7 = r.hincrby("bike:1", "price", -100)
print(res7)
# >>> 4972
# STEP_END

# REMOVE_START
assert res6 == 5072
assert res7 == 4972
# REMOVE_END


# STEP_START incrby_get_mget
res11 = r.hincrby("bike:1:stats", "rides", 1)
print(res11)
# >>> 1
res12 = r.hincrby("bike:1:stats", "rides", 1)
print(res12)
# >>> 2
res13 = r.hincrby("bike:1:stats", "rides", 1)
print(res13)
# >>> 3
res14 = r.hincrby("bike:1:stats", "crashes", 1)
print(res14)
# >>> 1
res15 = r.hincrby("bike:1:stats", "owners", 1)
print(res15)
# >>> 1
res16 = r.hget("bike:1:stats", "rides")
print(res16)
# >>> 3
res17 = r.hmget("bike:1:stats", ["crashes", "owners"])
print(res17)
# >>> ['1', '1']
# STEP_END

# REMOVE_START
assert res11 == 1
assert res12 == 2
assert res13 == 3
assert res14 == 1
assert res15 == 1
assert res16 == "3"
assert res17 == ["1", "1"]
# REMOVE_END
