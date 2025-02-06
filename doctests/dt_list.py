# EXAMPLE: list_tutorial
# HIDE_START
"""
Code samples for List doc pages:
    https://redis.io/docs/latest/develop/data-types/lists/
"""

import redis

r = redis.Redis(decode_responses=True)
# HIDE_END
# REMOVE_START
r.delete("bikes:repairs")
r.delete("bikes:finished")
# REMOVE_END

# STEP_START queue
res1 = r.lpush("bikes:repairs", "bike:1")
print(res1)  # >>> 1

res2 = r.lpush("bikes:repairs", "bike:2")
print(res2)  # >>> 2

res3 = r.rpop("bikes:repairs")
print(res3)  # >>> bike:1

res4 = r.rpop("bikes:repairs")
print(res4)  # >>> bike:2
# STEP_END

# REMOVE_START
assert res1 == 1
assert res2 == 2
assert res3 == "bike:1"
assert res4 == "bike:2"
# REMOVE_END

# STEP_START stack
res5 = r.lpush("bikes:repairs", "bike:1")
print(res5)  # >>> 1

res6 = r.lpush("bikes:repairs", "bike:2")
print(res6)  # >>> 2

res7 = r.lpop("bikes:repairs")
print(res7)  # >>> bike:2

res8 = r.lpop("bikes:repairs")
print(res8)  # >>> bike:1
# STEP_END

# REMOVE_START
assert res5 == 1
assert res6 == 2
assert res7 == "bike:2"
assert res8 == "bike:1"
# REMOVE_END

# STEP_START llen
res9 = r.llen("bikes:repairs")
print(res9)  # >>> 0
# STEP_END

# REMOVE_START
assert res9 == 0
# REMOVE_END

# STEP_START lmove_lrange
res10 = r.lpush("bikes:repairs", "bike:1")
print(res10)  # >>> 1

res11 = r.lpush("bikes:repairs", "bike:2")
print(res11)  # >>> 2

res12 = r.lmove("bikes:repairs", "bikes:finished", "LEFT", "LEFT")
print(res12)  # >>> 'bike:2'

res13 = r.lrange("bikes:repairs", 0, -1)
print(res13)  # >>> ['bike:1']

res14 = r.lrange("bikes:finished", 0, -1)
print(res14)  # >>> ['bike:2']
# STEP_END

# REMOVE_START
assert res10 == 1
assert res11 == 2
assert res12 == "bike:2"
assert res13 == ["bike:1"]
assert res14 == ["bike:2"]
r.delete("bikes:repairs")
# REMOVE_END

# STEP_START lpush_rpush
res15 = r.rpush("bikes:repairs", "bike:1")
print(res15)  # >>> 1

res16 = r.rpush("bikes:repairs", "bike:2")
print(res16)  # >>> 2

res17 = r.lpush("bikes:repairs", "bike:important_bike")
print(res17)  # >>> 3

res18 = r.lrange("bikes:repairs", 0, -1)
print(res18)  # >>> ['bike:important_bike', 'bike:1', 'bike:2']
# STEP_END

# REMOVE_START
assert res15 == 1
assert res16 == 2
assert res17 == 3
assert res18 == ["bike:important_bike", "bike:1", "bike:2"]
r.delete("bikes:repairs")
# REMOVE_END

# STEP_START variadic
res19 = r.rpush("bikes:repairs", "bike:1", "bike:2", "bike:3")
print(res19)  # >>> 3

res20 = r.lpush("bikes:repairs", "bike:important_bike", "bike:very_important_bike")
print(res20)  # >>> 5

res21 = r.lrange("bikes:repairs", 0, -1)
print(
    res21
)  # >>> ['bike:very_important_bike', 'bike:important_bike', 'bike:1', ...
# STEP_END

# REMOVE_START
assert res19 == 3
assert res20 == 5
assert res21 == [
    "bike:very_important_bike",
    "bike:important_bike",
    "bike:1",
    "bike:2",
    "bike:3",
]
r.delete("bikes:repairs")
# REMOVE_END

# STEP_START lpop_rpop
res22 = r.rpush("bikes:repairs", "bike:1", "bike:2", "bike:3")
print(res22)  # >>> 3

res23 = r.rpop("bikes:repairs")
print(res23)  # >>> 'bike:3'

res24 = r.lpop("bikes:repairs")
print(res24)  # >>> 'bike:1'

res25 = r.rpop("bikes:repairs")
print(res25)  # >>> 'bike:2'

res26 = r.rpop("bikes:repairs")
print(res26)  # >>> None
# STEP_END

# REMOVE_START
assert res22 == 3
assert res23 == "bike:3"
assert res24 == "bike:1"
assert res25 == "bike:2"
assert res26 is None
# REMOVE_END

# STEP_START ltrim
res27 = r.rpush("bikes:repairs", "bike:1", "bike:2", "bike:3", "bike:4", "bike:5")
print(res27)  # >>> 5

res28 = r.ltrim("bikes:repairs", 0, 2)
print(res28)  # >>> True

res29 = r.lrange("bikes:repairs", 0, -1)
print(res29)  # >>> ['bike:1', 'bike:2', 'bike:3']
# STEP_END

# REMOVE_START
assert res27 == 5
assert res28 is True
assert res29 == ["bike:1", "bike:2", "bike:3"]
r.delete("bikes:repairs")
# REMOVE_END

# STEP_START ltrim_end_of_list
res27 = r.rpush("bikes:repairs", "bike:1", "bike:2", "bike:3", "bike:4", "bike:5")
print(res27)  # >>> 5

res28 = r.ltrim("bikes:repairs", -3, -1)
print(res28)  # >>> True

res29 = r.lrange("bikes:repairs", 0, -1)
print(res29)  # >>> ['bike:3', 'bike:4', 'bike:5']
# STEP_END

# REMOVE_START
assert res27 == 5
assert res28 is True
assert res29 == ["bike:3", "bike:4", "bike:5"]
r.delete("bikes:repairs")
# REMOVE_END

# STEP_START brpop
res31 = r.rpush("bikes:repairs", "bike:1", "bike:2")
print(res31)  # >>> 2

res32 = r.brpop("bikes:repairs", timeout=1)
print(res32)  # >>> ('bikes:repairs', 'bike:2')

res33 = r.brpop("bikes:repairs", timeout=1)
print(res33)  # >>> ('bikes:repairs', 'bike:1')

res34 = r.brpop("bikes:repairs", timeout=1)
print(res34)  # >>> None
# STEP_END

# REMOVE_START
assert res31 == 2
assert res32 == ("bikes:repairs", "bike:2")
assert res33 == ("bikes:repairs", "bike:1")
assert res34 is None
r.delete("bikes:repairs")
r.delete("new_bikes")
# REMOVE_END

# STEP_START rule_1
res35 = r.delete("new_bikes")
print(res35)  # >>> 0

res36 = r.lpush("new_bikes", "bike:1", "bike:2", "bike:3")
print(res36)  # >>> 3
# STEP_END

# REMOVE_START
assert res35 == 0
assert res36 == 3
r.delete("new_bikes")
# REMOVE_END

# STEP_START rule_1.1
res37 = r.set("new_bikes", "bike:1")
print(res37)  # >>> True

res38 = r.type("new_bikes")
print(res38)  # >>> 'string'

try:
    res39 = r.lpush("new_bikes", "bike:2", "bike:3")
    # >>> redis.exceptions.ResponseError:
    # >>> WRONGTYPE Operation against a key holding the wrong kind of value
except redis.exceptions.ResponseError as e:
    print(e)
# STEP_END

# REMOVE_START
assert res37 is True
assert res38 == "string"
r.delete("new_bikes")
# REMOVE_END

# STEP_START rule_2
r.lpush("bikes:repairs", "bike:1", "bike:2", "bike:3")
print(res36)  # >>> 3

res40 = r.exists("bikes:repairs")
print(res40)  # >>> 1

res41 = r.lpop("bikes:repairs")
print(res41)  # >>> 'bike:3'

res42 = r.lpop("bikes:repairs")
print(res42)  # >>> 'bike:2'

res43 = r.lpop("bikes:repairs")
print(res43)  # >>> 'bike:1'

res44 = r.exists("bikes:repairs")
print(res44)  # >>> False
# STEP_END

# REMOVE_START
assert res40 == 1
assert res41 == "bike:3"
assert res42 == "bike:2"
assert res43 == "bike:1"
assert res44 == 0
r.delete("bikes:repairs")
# REMOVE_END

# STEP_START rule_3
res45 = r.delete("bikes:repairs")
print(res45)  # >>> 0

res46 = r.llen("bikes:repairs")
print(res46)  # >>> 0

res47 = r.lpop("bikes:repairs")
print(res47)  # >>> None
# STEP_END

# REMOVE_START
assert res45 == 0
assert res46 == 0
assert res47 is None
# REMOVE_END

# STEP_START ltrim.1
res48 = r.lpush("bikes:repairs", "bike:1", "bike:2", "bike:3", "bike:4", "bike:5")
print(res48)  # >>> 5

res49 = r.ltrim("bikes:repairs", 0, 2)
print(res49)  # >>> True

res50 = r.lrange("bikes:repairs", 0, -1)
print(res50)  # >>> ['bike:5', 'bike:4', 'bike:3']
# STEP_END

# REMOVE_START
assert res48 == 5
assert res49 is True
assert res50 == ["bike:5", "bike:4", "bike:3"]
r.delete("bikes:repairs")
# REMOVE_END
