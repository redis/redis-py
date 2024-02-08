# EXAMPLE: set_and_get
# HIDE_START
import redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
# HIDE_END

res = r.set("bike:1", "Process 134")
print(res)
# >>> True
# REMOVE_START
assert res
# REMOVE_END

res = r.get("bike:1")
print(res)
# >>> "Process 134"
# REMOVE_START
assert res == "Process 134"
# REMOVE_END
