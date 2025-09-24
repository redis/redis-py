# EXAMPLE: bitmap_tutorial
# HIDE_START
"""
Code samples for Bitmap doc pages:
    https://redis.io/docs/latest/develop/data-types/bitmaps/
"""
import redis

# Connect without the usual `decode_responses=True` to
# see the binary values in the responses more easily.
r = redis.Redis()
# HIDE_END

# REMOVE_START
r.delete("pings:2024-01-01-00:00", "A", "B", "C", "R")
# REMOVE_END

# STEP_START ping
res1 = r.setbit("pings:2024-01-01-00:00", 123, 1)
print(res1)  # >>> 0

res2 = r.getbit("pings:2024-01-01-00:00", 123)
print(res2)  # >>> 1

res3 = r.getbit("pings:2024-01-01-00:00", 456)
print(res3)  # >>> 0
# STEP_END

# REMOVE_START
assert res1 == 0
# REMOVE_END

# STEP_START bitcount
# HIDE_START
r.setbit("pings:2024-01-01-00:00", 123, 1)
# HIDE_END
res4 = r.bitcount("pings:2024-01-01-00:00")
print(res4)  # >>> 1
# STEP_END
# REMOVE_START
assert res4 == 1
# REMOVE_END

# STEP_START bitop_setup
r.setbit("A", 0, 1)
r.setbit("A", 1, 1)
r.setbit("A", 3, 1)
r.setbit("A", 4, 1)

res5 = r.get("A")
print("{:08b}".format(int.from_bytes(res5, "big")))
# >>> 11011000

r.setbit("B", 3, 1)
r.setbit("B", 4, 1)
r.setbit("B", 7, 1)

res6 = r.get("B")
print("{:08b}".format(int.from_bytes(res6, "big")))
# >>> 00011001

r.setbit("C", 1, 1)
r.setbit("C", 2, 1)
r.setbit("C", 4, 1)
r.setbit("C", 5, 1)

res7 = r.get("C")
print("{:08b}".format(int.from_bytes(res7, "big")))
# >>> 01101100
# STEP_END
# REMOVE_START
assert int.from_bytes(res5, "big") == 0b11011000
assert int.from_bytes(res6, "big") == 0b00011001
assert int.from_bytes(res7, "big") == 0b01101100
# REMOVE_END

# STEP_START bitop_and
r.bitop("AND", "R", "A", "B", "C")
res8 = r.get("R")
print("{:08b}".format(int.from_bytes(res8, "big")))
# >>> 00001000
# STEP_END
# REMOVE_START
assert int.from_bytes(res8, "big") == 0b00001000
# REMOVE_END

# STEP_START bitop_or
r.bitop("OR", "R", "A", "B", "C")
res9 = r.get("R")
print("{:08b}".format(int.from_bytes(res9, "big")))
# >>> 11111101
# STEP_END
# REMOVE_START
assert int.from_bytes(res9, "big") == 0b11111101
# REMOVE_END

# STEP_START bitop_xor
r.bitop("XOR", "R", "A", "B")
res10 = r.get("R")
print("{:08b}".format(int.from_bytes(res10, "big")))
# >>> 11000001
# STEP_END
# REMOVE_START
assert int.from_bytes(res10, "big") == 0b11000001
# REMOVE_END

# STEP_START bitop_not
r.bitop("NOT", "R", "A")
res11 = r.get("R")
print("{:08b}".format(int.from_bytes(res11, "big")))
# >>> 00100111
# STEP_END
# REMOVE_START
assert int.from_bytes(res11, "big") == 0b00100111
# REMOVE_END

# STEP_START bitop_diff
r.bitop("DIFF", "R", "A", "B", "C")
res12 = r.get("R")
print("{:08b}".format(int.from_bytes(res12, "big")))
# >>> 10000000
# STEP_END
# REMOVE_START
assert int.from_bytes(res12, "big") == 0b10000000
# REMOVE_END

# STEP_START bitop_diff1
r.bitop("DIFF1", "R", "A", "B", "C")
res13 = r.get("R")
print("{:08b}".format(int.from_bytes(res13, "big")))
# >>> 00100101
# STEP_END
# REMOVE_START
assert int.from_bytes(res13, "big") == 0b00100101
# REMOVE_END

# STEP_START bitop_andor
r.bitop("ANDOR", "R", "A", "B", "C")
res14 = r.get("R")
print("{:08b}".format(int.from_bytes(res14, "big")))
# >>> 01011000
# STEP_END
# REMOVE_START
assert int.from_bytes(res14, "big") == 0b01011000
# REMOVE_END

# STEP_START bitop_one
r.bitop("ONE", "R", "A", "B", "C")
res15 = r.get("R")
print("{:08b}".format(int.from_bytes(res15, "big")))
# >>> 10100101
# STEP_END
# REMOVE_START
assert int.from_bytes(res15, "big") == 0b10100101
# REMOVE_END
