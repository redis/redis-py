# EXAMPLE: cmds_list
# HIDE_START
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# STEP_START lpush
res1 = r.lpush("mylist", "world")
print(res1) # >>> 1

res2 = r.lpush("mylist", "hello")
print(res2) # >>> 2

res3 = r.lrange("mylist", 0, -1)
print(res3)  # >>> [ "hello", "world" ]

# REMOVE_START
assert res3 == [ "hello", "world" ]
r.delete("mylist")
# REMOVE_END
# STEP_END

# STEP_START lrange
res4 = r.rpush("mylist", "one");
print(res4) # >>> 1

res5 = r.rpush("mylist", "two")
print(res5) # >>> 2

res6 = r.rpush("mylist", "three")
print(res6) # >>> 3

res7 = r.lrange('mylist', 0, 0)
print(res7) # >>> [ 'one' ]

res8 = r.lrange('mylist', -3, 2)
print(res8) # >>> [ 'one', 'two', 'three' ]

res9 = r.lrange('mylist', -100, 100)
print(res9) # >>> [ 'one', 'two', 'three' ]

res10 = r.lrange('mylist', 5, 10)
print(res10) # >>> []

# REMOVE_START
assert res7 == [ 'one' ]
assert res8 == [ 'one', 'two', 'three' ]
assert res9 == [ 'one', 'two', 'three' ]
assert res10 == []
r.delete('mylist')
# REMOVE_END
# STEP_END

# STEP_START llen
res11 = r.lpush("mylist", "World")
print(res11) # >>> 1

res12 = r.lpush("mylist", "Hello")
print(res12) # >>> 2

res13 = r.llen("mylist")
print(res13)  # >>> 2

# REMOVE_START
assert res13 == 2
r.delete("mylist")
# REMOVE_END
# STEP_END

# STEP_START rpush
res14 = r.rpush("mylist", "hello")
print(res14) # >>> 1

res15 = r.rpush("mylist", "world")
print(res15) # >>> 2

res16 = r.lrange("mylist", 0, -1)
print(res16)  # >>> [ "hello", "world" ]

# REMOVE_START
assert res16 == [ "hello", "world" ]
r.delete("mylist")
# REMOVE_END
# STEP_END

# STEP_START lpop
res17 = r.rpush("mylist", *["one", "two", "three", "four", "five"])
print(res17) # >>> 5

res18 = r.lpop("mylist")
print(res18) # >>> "one"

res19 = r.lpop("mylist", 2)
print(res19) # >>> ['two', 'three']

res17 = r.lrange("mylist", 0, -1)
print(res17)  # >>> [ "four", "five" ]

# REMOVE_START
assert res17 == [ "four", "five" ]
r.delete("mylist")
# REMOVE_END
# STEP_END

# STEP_START rpop
res18 = r.rpush("mylist", *["one", "two", "three", "four", "five"])
print(res18) # >>> 5

res19 = r.rpop("mylist")
print(res19) # >>> "five"

res20 = r.rpop("mylist", 2)
print(res20) # >>> ['four', 'three']

res21 = r.lrange("mylist", 0, -1)
print(res21)  # >>> [ "one", "two" ]

# REMOVE_START
assert res21 == [ "one", "two" ]
r.delete("mylist")
# REMOVE_END
# STEP_END