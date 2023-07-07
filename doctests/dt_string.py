# EXAMPLE: set_tutorial
# HIDE_START
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# STEP_START set_get
res1 = r.set("bike:1", "Deimos")
print(res1)  # True
res2 = r.get("bike:1")
print(res2)  # Deimos
# STEP_END

# REMOVE_START
assert res1 == True
assert res2 == "Deimos"
# REMOVE_END

# STEP_START setnx_xx
res3 = r.set("bike:1", "bike", nx=True)
print(res3)  # False
res4 = r.set("bike:1", "bike", xx=True)
print(res4)  # True
# STEP_END

# REMOVE_START
assert res3 == False
assert res4 == True
# REMOVE_END

# STEP_START mset
res5 = r.mset({"bike:1": "Deimos", "bike:2": "Ares", "bike:3": "Vanth"})
print(res5)  # True
res6 = r.mget(["bike:1", "bike:2", "bike:3"])
print(res6)  # ['Deimos', 'Ares', 'Vanth']
# STEP_END

# REMOVE_START
assert res5 == True
assert res6 == ["Deimos", "Ares", "Vanth"]
# REMOVE_END

# STEP_START incr
r.set("total_crashes", 0)
res7 = r.incr("total_crashes")
print(res7)  # 1
res8 = r.incrby("total_crashes", 10)
print(res8)  # 11
# STEP_END

# REMOVE_START
assert res7 == 1
assert res8 == 11
# REMOVE_END
