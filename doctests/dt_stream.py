# EXAMPLE: stream_tutorial
# HIDE_START
"""
Code samples for Stream doc pages:
    https://redis.io/docs/latest/develop/data-types/streams/
"""

import redis

r = redis.Redis(decode_responses=True)
# HIDE_END
# REMOVE_START
r.delete("race:france", "race:italy", "race:usa")
# REMOVE_END

# STEP_START xadd
res1 = r.xadd(
    "race:france",
    {"rider": "Castilla", "speed": 30.2, "position": 1, "location_id": 1},
)
print(res1)  # >>> 1692629576966-0

res2 = r.xadd(
    "race:france",
    {"rider": "Norem", "speed": 28.8, "position": 3, "location_id": 1},
)
print(res2)  # >>> 1692629594113-0

res3 = r.xadd(
    "race:france",
    {"rider": "Prickett", "speed": 29.7, "position": 2, "location_id": 1},
)
print(res3)  # >>> 1692629613374-0
# STEP_END

# REMOVE_START
assert r.xlen("race:france") == 3
# REMOVE_END

# STEP_START xrange
res4 = r.xrange("race:france", "1691765278160-0", "+", 2)
print(
    res4
)  # >>> [
#   ('1692629576966-0',
#       {'rider': 'Castilla', 'speed': '30.2', 'position': '1', 'location_id': '1'}
#   ),
#   ('1692629594113-0',
#       {'rider': 'Norem', 'speed': '28.8', 'position': '3', 'location_id': '1'}
#   )
# ]
# STEP_END

# STEP_START xread_block
res5 = r.xread(streams={"race:france": 0}, count=100, block=300)
print(
    res5
)
# >>> [
#   ['race:france',
#       [('1692629576966-0',
#           {'rider': 'Castilla', 'speed': '30.2', 'position': '1', 'location_id': '1'}
#       ),
#       ('1692629594113-0',
#           {'rider': 'Norem', 'speed': '28.8', 'position': '3', 'location_id': '1'}
#       ),
#       ('1692629613374-0',
#           {'rider': 'Prickett', 'speed': '29.7', 'position': '2', 'location_id': '1'}
#       )]
# ]
# ]
# STEP_END

# STEP_START xadd_2
res6 = r.xadd(
    "race:france",
    {"rider": "Castilla", "speed": 29.9, "position": 1, "location_id": 2},
)
print(res6)  # >>> 1692629676124-0
# STEP_END

# STEP_START xlen
res7 = r.xlen("race:france")
print(res7)  # >>> 4
# STEP_END


# STEP_START xadd_id
res8 = r.xadd("race:usa", {"racer": "Castilla"}, id="0-1")
print(res8)  # >>> 0-1

res9 = r.xadd("race:usa", {"racer": "Norem"}, id="0-2")
print(res9)  # >>> 0-2
# STEP_END

# STEP_START xadd_bad_id
try:
    res10 = r.xadd("race:usa", {"racer": "Prickett"}, id="0-1")
    print(res10)  # >>> 0-1
except redis.exceptions.ResponseError as e:
    print(e)  # >>> WRONGID
# STEP_END

# STEP_START xadd_7
# Not yet implemented
# STEP_END

# STEP_START xrange_all
res11 = r.xrange("race:france", "-", "+")
print(
    res11
)
# >>> [
#   ('1692629576966-0',
#       {'rider': 'Castilla', 'speed': '30.2', 'position': '1', 'location_id': '1'}
#   ),
#   ('1692629594113-0',
#       {'rider': 'Norem', 'speed': '28.8', 'position': '3', 'location_id': '1'}
#   ),
#   ('1692629613374-0',
#       {'rider': 'Prickett', 'speed': '29.7', 'position': '2', 'location_id': '1'}
#   ),
#   ('1692629676124-0',
#       {'rider': 'Castilla', 'speed': '29.9', 'position': '1', 'location_id': '2'}
#   )
# ]
# STEP_END

# STEP_START xrange_time
res12 = r.xrange("race:france", 1692629576965, 1692629576967)
print(
    res12
)
# >>> [
#       ('1692629576966-0',
#           {'rider': 'Castilla', 'speed': '30.2', 'position': '1', 'location_id': '1'}
#       )
# ]
# STEP_END

# STEP_START xrange_step_1
res13 = r.xrange("race:france", "-", "+", 2)
print(
    res13
)
# >>> [
#   ('1692629576966-0',
#       {'rider': 'Castilla', 'speed': '30.2', 'position': '1', 'location_id': '1'}
#   ),
#   ('1692629594113-0',
#       {'rider': 'Norem', 'speed': '28.8', 'position': '3', 'location_id': '1'}
#   )
# ]
# STEP_END

# STEP_START xrange_step_2
res14 = r.xrange("race:france", "(1692629594113-0", "+", 2)
print(
    res14
)
# >>> [
#   ('1692629613374-0',
#       {'rider': 'Prickett', 'speed': '29.7', 'position': '2', 'location_id': '1'}
#   ),
#   ('1692629676124-0',
#       {'rider': 'Castilla', 'speed': '29.9', 'position': '1', 'location_id': '2'}
#   )
# ]
# STEP_END

# STEP_START xrange_empty
res15 = r.xrange("race:france", "(1692629676124-0", "+", 2)
print(res15)  # >>> []
# STEP_END

# STEP_START xrevrange
res16 = r.xrevrange("race:france", "+", "-", 1)
print(
    res16
)
# >>> [
#       ('1692629676124-0',
#           {'rider': 'Castilla', 'speed': '29.9', 'position': '1', 'location_id': '2'}
#       )
# ]
# STEP_END

# STEP_START xread
res17 = r.xread(streams={"race:france": 0}, count=2)
print(
    res17
)
# >>> [
#       ['race:france', [
#       ('1692629576966-0',
#           {'rider': 'Castilla', 'speed': '30.2', 'position': '1', 'location_id': '1'}
#       ),
#       ('1692629594113-0',
#           {'rider': 'Norem', 'speed': '28.8', 'position': '3', 'location_id': '1'}
#       )
#       ]
#       ]
#   ]
# STEP_END

# STEP_START xgroup_create
res18 = r.xgroup_create("race:france", "france_riders", "$")
print(res18)  # >>> True
# STEP_END

# STEP_START xgroup_create_mkstream
res19 = r.xgroup_create("race:italy", "italy_riders", "$", mkstream=True)
print(res19)  # >>> True
# STEP_END

# STEP_START xgroup_read
r.xadd("race:italy", {"rider": "Castilla"})
r.xadd("race:italy", {"rider": "Royce"})
r.xadd("race:italy", {"rider": "Sam-Bodden"})
r.xadd("race:italy", {"rider": "Prickett"})
r.xadd("race:italy", {"rider": "Norem"})

res20 = r.xreadgroup(
    streams={"race:italy": ">"},
    consumername="Alice",
    groupname="italy_riders",
    count=1,
)
print(res20)  # >>> [['race:italy', [('1692629925771-0', {'rider': 'Castilla'})]]]
# STEP_END

# STEP_START xgroup_read_id
res21 = r.xreadgroup(
    streams={"race:italy": 0},
    consumername="Alice",
    groupname="italy_riders",
    count=1,
)
print(res21)  # >>> [['race:italy', [('1692629925771-0', {'rider': 'Castilla'})]]]
# STEP_END

# STEP_START xack
res22 = r.xack("race:italy", "italy_riders", "1692629925771-0")
print(res22)  # >>> 1

res23 = r.xreadgroup(
    streams={"race:italy": 0},
    consumername="Alice",
    groupname="italy_riders",
    count=1,
)
print(res23)  # >>> [['race:italy', []]]
# STEP_END

# STEP_START xgroup_read_bob
res24 = r.xreadgroup(
    streams={"race:italy": ">"},
    consumername="Bob",
    groupname="italy_riders",
    count=2,
)
print(
    res24
)
# >>> [
#       ['race:italy', [
#           ('1692629925789-0',
#               {'rider': 'Royce'}
#           ),
#           ('1692629925790-0',
#               {'rider': 'Sam-Bodden'}
#           )
#       ]
#       ]
# ]
# STEP_END

# STEP_START xpending
res25 = r.xpending("race:italy", "italy_riders")
print(
    res25
)
# >>> {
#       'pending': 2, 'min': '1692629925789-0', 'max': '1692629925790-0',
#       'consumers': [{'name': 'Bob', 'pending': 2}]
# }
# STEP_END

# STEP_START xpending_plus_minus
res26 = r.xpending_range("race:italy", "italy_riders", "-", "+", 10)
print(
    res26
)
# >>> [
#       {
#           'message_id': '1692629925789-0', 'consumer': 'Bob',
#           'time_since_delivered': 31084, 'times_delivered': 1
#       },
#       {
#           'message_id': '1692629925790-0', 'consumer': 'Bob',
#           'time_since_delivered': 31084, 'times_delivered': 1
#       }
# ]
# STEP_END

# STEP_START xrange_pending
res27 = r.xrange("race:italy", "1692629925789-0", "1692629925789-0")
print(res27)  # >>> [('1692629925789-0', {'rider': 'Royce'})]
# STEP_END

# STEP_START xclaim
res28 = r.xclaim("race:italy", "italy_riders", "Alice", 60000, ["1692629925789-0"])
print(res28)  # >>> [('1692629925789-0', {'rider': 'Royce'})]
# STEP_END

# STEP_START xautoclaim
res29 = r.xautoclaim("race:italy", "italy_riders", "Alice", 1, "0-0", 1)
print(res29)  # >>> ['1692629925790-0', [('1692629925789-0', {'rider': 'Royce'})]]
# STEP_END

# STEP_START xautoclaim_cursor
res30 = r.xautoclaim("race:italy", "italy_riders", "Alice", 1, "(1692629925789-0", 1)
print(res30)  # >>> ['0-0', [('1692629925790-0', {'rider': 'Sam-Bodden'})]]
# STEP_END

# STEP_START xinfo
res31 = r.xinfo_stream("race:italy")
print(
    res31
)
# >>> {
#       'length': 5, 'radix-tree-keys': 1, 'radix-tree-nodes': 2,
#       'last-generated-id': '1692629926436-0', 'groups': 1,
#       'first-entry': ('1692629925771-0', {'rider': 'Castilla'}),
#       'last-entry': ('1692629926436-0', {'rider': 'Norem'})
# }
# STEP_END

# STEP_START xinfo_groups
res32 = r.xinfo_groups("race:italy")
print(
    res32
)
# >>> [
#       {
#           'name': 'italy_riders', 'consumers': 2, 'pending': 2,
#           'last-delivered-id': '1692629925790-0'
#       }
# ]
# STEP_END

# STEP_START xinfo_consumers
res33 = r.xinfo_consumers("race:italy", "italy_riders")
print(
    res33
)
# >>> [
#       {'name': 'Alice', 'pending': 2, 'idle': 199332},
#       {'name': 'Bob', 'pending': 0, 'idle': 489170}
# ]
# STEP_END

# STEP_START maxlen
r.xadd("race:italy", {"rider": "Jones"}, maxlen=2)
r.xadd("race:italy", {"rider": "Wood"}, maxlen=2)
r.xadd("race:italy", {"rider": "Henshaw"}, maxlen=2)

res34 = r.xlen("race:italy")
print(res34)  # >>> 8

res35 = r.xrange("race:italy", "-", "+")
print(
    res35
)
# >>> [
#       ('1692629925771-0', {'rider': 'Castilla'}),
#       ('1692629925789-0', {'rider': 'Royce'}),
#       ('1692629925790-0', {'rider': 'Sam-Bodden'}),
#       ('1692629925791-0', {'rider': 'Prickett'}),
#       ('1692629926436-0', {'rider': 'Norem'}),
#       ('1692630612602-0', {'rider': 'Jones'}),
#       ('1692630641947-0', {'rider': 'Wood'}),
#       ('1692630648281-0', {'rider': 'Henshaw'})
# ]

r.xadd("race:italy", {"rider": "Smith"}, maxlen=2, approximate=False)

res36 = r.xrange("race:italy", "-", "+")
print(
    res36
)
# >>> [
#       ('1692630648281-0', {'rider': 'Henshaw'}),
#       ('1692631018238-0', {'rider': 'Smith'})
# ]
# STEP_END

# STEP_START xtrim
res37 = r.xtrim("race:italy", maxlen=10, approximate=False)
print(res37)  # >>> 0
# STEP_END

# STEP_START xtrim2
res38 = r.xtrim("race:italy", maxlen=10)
print(res38)  # >>> 0
# STEP_END

# STEP_START xdel
res39 = r.xrange("race:italy", "-", "+")
print(
    res39
)
# >>> [
#       ('1692630648281-0', {'rider': 'Henshaw'}),
#       ('1692631018238-0', {'rider': 'Smith'})
# ]

res40 = r.xdel("race:italy", "1692631018238-0")
print(res40)  # >>> 1

res41 = r.xrange("race:italy", "-", "+")
print(res41)  # >>> [('1692630648281-0', {'rider': 'Henshaw'})]
# STEP_END
