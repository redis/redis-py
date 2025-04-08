# EXAMPLE: vecset_tutorial
# HIDE_START
"""
Code samples for Vector set doc pages:
    https://redis.io/docs/latest/develop/data-types/vector-sets/
"""

import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# REMOVE_START
r.delete("points")
# REMOVE_END

# STEP_START vadd
res1 = r.vset().vadd("points", [1.0, 1.0], "pt:A")
print(res1)  # >>> 1

res2 = r.vset().vadd("points", [-1.0, -1.0], "pt:B")
print(res2)  # >>> 1

res3 = r.vset().vadd("points", [-1.0, 1.0], "pt:C")
print(res3)  # >>> 1

res4 = r.vset().vadd("points", [1.0, -1.0], "pt:D")
print(res4)  # >>> 1

res5 = r.vset().vadd("points", [1.0, 0], "pt:E")
print(res5)  # >>> 1

res6 = r.type("points")
print(res6)  # >>> vectorset
# STEP_END
# REMOVE_START
assert res1 == 1
assert res2 == 1
assert res3 == 1
assert res4 == 1
assert res5 == 1

assert res6 == "vectorset"
# REMOVE_END

# STEP_START vcardvdim
res7 = r.vset().vcard("points")
print(res7)  # >>> 5

res8 = r.vset().vdim("points")
print(res8)  # >>> 2
# STEP_END
# REMOVE_START
assert res7 == 5
assert res8 == 2
# REMOVE_END

# STEP_START vemb
res9 = r.vset().vemb("points", "pt:A")
print(res9)  # >>> [0.9999999403953552, 0.9999999403953552]

res10 = r.vset().vemb("points", "pt:B")
print(res10)  # >>> [-0.9999999403953552, -0.9999999403953552]

res11 = r.vset().vemb("points", "pt:C")
print(res11)  # >>> [-0.9999999403953552, 0.9999999403953552]

res12 = r.vset().vemb("points", "pt:D")
print(res12)  # >>> [0.9999999403953552, -0.9999999403953552]

res13 = r.vset().vemb("points", "pt:E")
print(res13)  # >>> [1, 0]
# STEP_END
# REMOVE_START
assert 1 - res9[0] < 0.001
assert 1 - res9[1] < 0.001
assert 1 + res10[0] < 0.001
assert 1 + res10[1] < 0.001
assert 1 + res11[0] < 0.001
assert 1 - res11[1] < 0.001
assert 1 - res12[0] < 0.001
assert 1 + res12[1] < 0.001
assert res13 == [1, 0]
# REMOVE_END

# STEP_START attr
res14 = r.vset().vsetattr("points", "pt:A", {
    "name": "Point A",
    "description": "First point added"
})
print(res14)  # >>> 1

res15 = r.vset().vgetattr("points", "pt:A")
print(res15)
# >>> {'name': 'Point A', 'description': 'First point added'}

res16 = r.vset().vsetattr("points", "pt:A", "")
print(res16)  # >>> 1

res17 = r.vset().vgetattr("points", "pt:A")
print(res17)  # >>> None
# STEP_END
# REMOVE_START
assert res14 == 1
assert res15 == {"name": "Point A", "description": "First point added"}
assert res16 == 1
assert res17 is None
# REMOVE_END

# STEP_START vrem
res18 = r.vset().vadd("points", [0, 0], "pt:F")
print(res18)  # >>> 1

res19 = r.vset().vcard("points")
print(res19)  # >>> 6

res20 = r.vset().vrem("points", "pt:F")
print(res20)  # >>> 1

res21 = r.vset().vcard("points")
print(res21)  # >>> 5
# STEP_END
# REMOVE_START
assert res18 == 1
assert res19 == 6
assert res20 == 1
assert res21 == 5
# REMOVE_END

# STEP_START vsim_basic
res22 = r.vset().vsim("points", [0.9, 0.1])
print(res22)
# >>> ['pt:E', 'pt:A', 'pt:D', 'pt:C', 'pt:B']
# STEP_END
# REMOVE_START
assert res22 == ["pt:E", "pt:A", "pt:D", "pt:C", "pt:B"]
# REMOVE_END

# STEP_START vsim_options
res23 = r.vset().vsim(
    "points", "pt:A",
    with_scores=True,
    count=4
)
print(res23)
# >>> {'pt:A': 1.0, 'pt:E': 0.8535534143447876, 'pt:D': 0.5, 'pt:C': 0.5}
# STEP_END
# REMOVE_START
assert res23["pt:A"] == 1.0
assert res23["pt:C"] == 0.5
assert res23["pt:D"] == 0.5
assert res23["pt:E"] - 0.85 < 0.005
# REMOVE_END

# STEP_START vsim_filter
res24 = r.vset().vsetattr("points", "pt:A", {
    "size": "large",
    "price": 18.99
})
print(res24)  # >>> 1

res25 = r.vset().vsetattr("points", "pt:B", {
    "size": "large",
    "price": 35.99
})
print(res25)  # >>> 1

res26 = r.vset().vsetattr("points", "pt:C", {
    "size": "large",
    "price": 25.99
})
print(res26)  # >>> 1

res27 = r.vset().vsetattr("points", "pt:D", {
    "size": "small",
    "price": 21.00
})
print(res27)  # >>> 1

res28 = r.vset().vsetattr("points", "pt:E", {
    "size": "small",
    "price": 17.75
})
print(res28)  # >>> 1

# Return elements in order of distance from point A whose
# `size` attribute is `large`.
res29 = r.vset().vsim(
    "points", "pt:A",
    filter='.size == "large"'
)
print(res29)  # >>> ['pt:A', 'pt:C', 'pt:B']

# Return elements in order of distance from point A whose size is
# `large` and whose price is greater than 20.00.
res30 = r.vset().vsim(
    "points", "pt:A",
    filter='.size == "large" && .price > 20.00'
)
print(res30)  # >>> ['pt:C', 'pt:B']
# STEP_END
# REMOVE_START
assert res24 == 1
assert res25 == 1
assert res26 == 1
assert res27 == 1
assert res28 == 1

assert res30 == ['pt:C', 'pt:B']
# REMOVE_END
