# EXAMPLE: query_ft
# HIDE_START
import json
import sys
import redis
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import NumericFilter, Query

r = redis.Redis(decode_responses=True)

# create index
schema = (
    TextField("$.brand", as_name="brand"),
    TextField("$.model", as_name="model"),
    TextField("$.description", as_name="description"),
)

index = r.ft("idx:bicycle")
index.create_index(
    schema,
    definition=IndexDefinition(prefix=["bicycle:"], index_type=IndexType.JSON),
)

# load data
with open("data/query_em.json") as f:
    bicycles = json.load(f)

pipeline = r.pipeline(transaction=False)
for bid, bicycle in enumerate(bicycles):
    pipeline.json().set(f'bicycle:{bid}', Path.root_path(), bicycle)
pipeline.execute()
# HIDE_END

# STEP_START ft1
res = index.search(Query("@description: kids"))
print(res.total)
# >>> 2
# REMOVE_START
assert res.total == 2
# REMOVE_END
# STEP_END

# STEP_START ft2
res = index.search(Query("@model: ka*"))
print(res.total)
# >>> 1
# REMOVE_START
assert res.total == 1
# REMOVE_END
# STEP_END

# STEP_START ft3
res = index.search(Query("@brand: *bikes"))
print(res.total)
# >>> 2
# REMOVE_START
assert res.total == 2
# REMOVE_END
# STEP_END

# STEP_START ft4
res = index.search(Query("%optamized%"))
print(res)
# >>> Result{1 total, docs: [Document {'id': 'bicycle:3', 'payload': None, 'json': '{"pickup_zone":"POLYGON((-80.2433 25.8067, -80.1333 25.8067, -80.1333 25.6967, -80.2433 25.6967, -80.2433 25.8067))","store_location":"-80.1918,25.7617","brand":"Eva","model":"Eva 291","price":3400,"description":"The sister company to Nord, Eva launched in 2005 as the first and only women-dedicated bicycle brand. Designed by women for women, allEva bikes are optimized for the feminine physique using analytics from a body metrics database. If you like 29ers, try the Eva 291. It’s a brand new bike for 2022.. This full-suspension, cross-country ride has been designed for velocity. The 291 has 100mm of front and rear travel, a superlight aluminum frame and fast-rolling 29-inch wheels. Yippee!","condition":"used"}'}]}
# REMOVE_START
assert res.total == 1
# REMOVE_END
# STEP_END

# STEP_START ft5
res = index.search(Query("%%optamised%%"))
print(res)
# >>> Result{1 total, docs: [Document {'id': 'bicycle:3', 'payload': None, 'json': '{"pickup_zone":"POLYGON((-80.2433 25.8067, -80.1333 25.8067, -80.1333 25.6967, -80.2433 25.6967, -80.2433 25.8067))","store_location":"-80.1918,25.7617","brand":"Eva","model":"Eva 291","price":3400,"description":"The sister company to Nord, Eva launched in 2005 as the first and only women-dedicated bicycle brand. Designed by women for women, allEva bikes are optimized for the feminine physique using analytics from a body metrics database. If you like 29ers, try the Eva 291. It’s a brand new bike for 2022.. This full-suspension, cross-country ride has been designed for velocity. The 291 has 100mm of front and rear travel, a superlight aluminum frame and fast-rolling 29-inch wheels. Yippee!","condition":"used"}'}]}
# REMOVE_START
assert res.total == 1
# REMOVE_END
# STEP_END

# REMOVE_START
# destroy index and data
r.ft("idx:bicycle").dropindex(delete_documents=True)
# REMOVE_END
