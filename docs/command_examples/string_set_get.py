# HIDE_START
import redis

r = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True
)
# HIDE_END

res = r.set("foo", "bar")
print(res)
# >>> True
# REMOVE_START
assert res
# REMOVE_END

res = r.get("foo")
print(res)
# >>> "bar"
# REMOVE_START
assert res == "bar"
# REMOVE_END
