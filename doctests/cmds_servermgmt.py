# EXAMPLE: cmds_servermgmt
# HIDE_START
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# STEP_START flushall
# REMOVE_START
r.set("foo", "1")
r.set("bar", "2")
r.set("baz", "3")
# REMOVE_END
res1 = r.flushall(asynchronous=False)
print(res1) # >>> True

res2 = r.keys()
print(res2) # >>> []

# REMOVE_START
assert res1 == True
assert res2 == []
# REMOVE_END
# STEP_END

# STEP_START info
res3 = r.info()
print(res3)
# >>> {'redis_version': '7.4.0', 'redis_git_sha1': 'c9d29f6a',...}
# STEP_END