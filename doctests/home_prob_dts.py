# EXAMPLE: home_prob_dts
"""
Probabilistic data type examples:
 https://redis.io/docs/latest/develop/connect/clients/python/redis-py/prob
"""

# HIDE_START
import redis
r = redis.Redis(decode_responses=True)
# HIDE_END
# REMOVE_START
r.delete(
    "recorded_users", "other_users",
    "group:1", "group:2", "both_groups",
    "items_sold",
    "male_heights", "female_heights", "all_heights",
    "top_3_songs"
)
# REMOVE_END

# STEP_START bloom
res1 = r.bf().madd("recorded_users", "andy", "cameron", "david", "michelle")
print(res1)  # >>> [1, 1, 1, 1]

res2 = r.bf().exists("recorded_users", "cameron")
print(res2)  # >>> 1

res3 = r.bf().exists("recorded_users", "kaitlyn")
print(res3)  # >>> 0
# STEP_END
# REMOVE_START
assert res1 == [1, 1, 1, 1]
assert res2 == 1
assert res3 == 0
# REMOVE_END

# STEP_START cuckoo
res4 = r.cf().add("other_users", "paolo")
print(res4)  # >>> 1

res5 = r.cf().add("other_users", "kaitlyn")
print(res5)  # >>> 1

res6 = r.cf().add("other_users", "rachel")
print(res6)  # >>> 1

res7 = r.cf().mexists("other_users", "paolo", "rachel", "andy")
print(res7)  # >>> [1, 1, 0]

res8 = r.cf().delete("other_users", "paolo")
print(res8)  # >>> 1

res9 = r.cf().exists("other_users", "paolo")
print(res9)  # >>> 0
# STEP_END
# REMOVE_START
assert res4 == 1
assert res5 == 1
assert res6 == 1
assert res7 == [1, 1, 0]
assert res8 == 1
assert res9 == 0
# REMOVE_END

# STEP_START hyperloglog
res10 = r.pfadd("group:1", "andy", "cameron", "david")
print(res10)  # >>> 1

res11 = r.pfcount("group:1")
print(res11)  # >>> 3

res12 = r.pfadd("group:2", "kaitlyn", "michelle", "paolo", "rachel")
print(res12)  # >>> 1

res13 = r.pfcount("group:2")
print(res13)  # >>> 4

res14 = r.pfmerge("both_groups", "group:1", "group:2")
print(res14)  # >>> True

res15 = r.pfcount("both_groups")
print(res15)  # >>> 7
# STEP_END
# REMOVE_START
assert res10 == 1
assert res11 == 3
assert res12 == 1
assert res13 == 4
assert res14
assert res15 == 7
# REMOVE_END

# STEP_START cms
# Specify that you want to keep the counts within 0.01
# (1%) of the true value with a 0.005 (0.5%) chance
# of going outside this limit.
res16 = r.cms().initbyprob("items_sold", 0.01, 0.005)
print(res16)  # >>> True

# The parameters for `incrby()` are two lists. The count
# for each item in the first list is incremented by the
# value at the same index in the second list.
res17 = r.cms().incrby(
    "items_sold",
    ["bread", "tea", "coffee", "beer"],  # Items sold
    [300, 200, 200, 100]
)
print(res17)  # >>> [300, 200, 200, 100]

res18 = r.cms().incrby(
    "items_sold",
    ["bread", "coffee"],
    [100, 150]
)
print(res18)  # >>> [400, 350]

res19 = r.cms().query("items_sold", "bread", "tea", "coffee", "beer")
print(res19)  # >>> [400, 200, 350, 100]
# STEP_END
# REMOVE_START
assert res16
assert res17 == [300, 200, 200, 100]
assert res18 == [400, 350]
assert res19 == [400, 200, 350, 100]
# REMOVE_END

# STEP_START tdigest
res20 = r.tdigest().create("male_heights")
print(res20)  # >>> True

res21 = r.tdigest().add(
    "male_heights",
    [175.5, 181, 160.8, 152, 177, 196, 164]
)
print(res21)  # >>> OK

res22 = r.tdigest().min("male_heights")
print(res22)  # >>> 152.0

res23 = r.tdigest().max("male_heights")
print(res23)  # >>> 196.0

res24 = r.tdigest().quantile("male_heights", 0.75)
print(res24)  # >>> 181

# Note that the CDF value for 181 is not exactly
# 0.75. Both values are estimates.
res25 = r.tdigest().cdf("male_heights", 181)
print(res25)  # >>> [0.7857142857142857]

res26 = r.tdigest().create("female_heights")
print(res26)  # >>> True

res27 = r.tdigest().add(
    "female_heights",
    [155.5, 161, 168.5, 170, 157.5, 163, 171]
)
print(res27)  # >>> OK

res28 = r.tdigest().quantile("female_heights", 0.75)
print(res28)  # >>> [170]

res29 = r.tdigest().merge(
    "all_heights", 2, "male_heights", "female_heights"
)
print(res29)  # >>> OK

res30 = r.tdigest().quantile("all_heights", 0.75)
print(res30)  # >>> [175.5]
# STEP_END
# REMOVE_START
assert res20
assert res21 == "OK"
assert res22 == 152.0
assert res23 == 196.0
assert res24 == [181]
assert res25 == [0.7857142857142857]
assert res26
assert res27 == "OK"
assert res28 == [170]
assert res29 == "OK"
assert res30 == [175.5]
# REMOVE_END

# STEP_START topk
# The `reserve()` method creates the Top-K object with
# the given key. The parameters are the number of items
# in the ranking and values for `width`, `depth`, and
# `decay`, described in the Top-K reference page.
res31 = r.topk().reserve("top_3_songs", 3, 7, 8, 0.9)
print(res31)  # >>> True

# The parameters for `incrby()` are two lists. The count
# for each item in the first list is incremented by the
# value at the same index in the second list.
res32 = r.topk().incrby(
    "top_3_songs",
    [
        "Starfish Trooper",
        "Only one more time",
        "Rock me, Handel",
        "How will anyone know?",
        "Average lover",
        "Road to everywhere"
    ],
    [
        3000,
        1850,
        1325,
        3890,
        4098,
        770
    ]
)
print(res32)
# >>> [None, None, None, 'Rock me, Handel', 'Only one more time', None]

res33 = r.topk().list("top_3_songs")
print(res33)
# >>> ['Average lover', 'How will anyone know?', 'Starfish Trooper']

res34 = r.topk().query(
    "top_3_songs", "Starfish Trooper", "Road to everywhere"
)
print(res34)  # >>> [1, 0]
# STEP_END
# REMOVE_START
assert res31
assert res32 == [None, None, None, 'Rock me, Handel', 'Only one more time', None]
assert res33 == ['Average lover', 'How will anyone know?', 'Starfish Trooper']
assert res34 == [1, 0]
# REMOVE_END
