# EXAMPLE: cmds_string
# HIDE_START
import redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
# HIDE_END

# STEP_START incr
res = r.set("mykey", "10")
print(res)
# >>> True
res = r.incr("mykey")
print(res)
# >>> 11
# REMOVE_START
assert res == 11
r.delete("mykey")
# REMOVE_END
# STEP_END
