# EXAMPLE: query_geo
# HIDE_START
import json
import sys
import redis
from redis.commands.json.path import Path
from redis.commands.search.field import GeoField, GeoShapeField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

r = redis.Redis(decode_responses=True)

# create index
schema = (
    GeoField("$.store_location", as_name="store_location"),
    GeoShapeField("$.pickup_zone", coord_system=GeoShapeField.FLAT, as_name="pickup_zone")
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

# STEP_START geo1
params_dict = {"lon": -0.1778, "lat": 51.5524, "radius": 20, "units": "mi"}
q = Query("@store_location:[$lon $lat $radius $units]").dialect(2)
res = index.search(q, query_params=params_dict)
print(res)
# >>> Result{1 total, docs: [Document {'id': 'bicycle:5', ...
# REMOVE_START
assert res.total == 1
# REMOVE_END
# STEP_END

# STEP_START geo2
params_dict = {"bike": "POINT(-0.1278 51.5074)"}
q = Query("@pickup_zone:[CONTAINS $bike]").dialect(3)
res = index.search(q, query_params=params_dict)
print(res.total) # >>> 1
# REMOVE_START
assert res.total == 1
# REMOVE_END
# STEP_END

# STEP_START geo3
params_dict = {"europe": "POLYGON((-25 35, 40 35, 40 70, -25 70, -25 35))"}
q = Query("@pickup_zone:[WITHIN $europe]").dialect(3)
res = index.search(q, query_params=params_dict)
print(res.total) # >>> 5
# REMOVE_START
assert res.total == 5
# REMOVE_END
# STEP_END

# REMOVE_START
# destroy index and data
r.ft("idx:bicycle").dropindex(delete_documents=True)
# REMOVE_END
