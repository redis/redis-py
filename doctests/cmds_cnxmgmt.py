# EXAMPLE: cmds_cnxmgmt
# HIDE_START
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# STEP_START auth1
# REMOVE_START
r.config_set("requirepass", "temp_pass")
# REMOVE_END
res1 = r.auth(password="temp_pass")
print(res1) # >>> True

res2 = r.auth(password="temp_pass", username="default")
print(res2) # >>> True

# REMOVE_START
assert res1 == True
assert res2 == True
r.config_set("requirepass", "")
# REMOVE_END
# STEP_END

# STEP_START auth2
# REMOVE_START
r.acl_setuser("test-user", enabled=True, passwords=["+strong_password"], commands=["+acl"])
# REMOVE_END
res = r.auth(username="test-user", password="strong_password")
print(res) # >>> True

# REMOVE_START
assert res == True
r.acl_deluser("test-user")
# REMOVE_END
# STEP_END
