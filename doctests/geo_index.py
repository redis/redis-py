# EXAMPLE: geoindex
import redis
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, GeoField, GeoShapeField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

r = redis.Redis()
# REMOVE_START
try:
    r.ft("productidx").dropindex(True)
except redis.exceptions.ResponseError:
    pass

try:
    r.ft("geomidx").dropindex(True)
except redis.exceptions.ResponseError:
    pass

r.delete("product:46885", "product:46886", "shape:1", "shape:2", "shape:3", "shape:4")
# REMOVE_END

# STEP_START create_geo_idx
geo_schema = (
    GeoField("$.location", as_name="location")
)

geo_index_create_result = r.ft("productidx").create_index(
    geo_schema,
    definition=IndexDefinition(
        prefix=["product:"], index_type=IndexType.JSON
    )
)
print(geo_index_create_result)  # >>> True
# STEP_END
# REMOVE_START
assert geo_index_create_result
# REMOVE_END

# STEP_START add_geo_json
prd46885 = {
    "description": "Navy Blue Slippers",
    "price": 45.99,
    "city": "Denver",
    "location": "-104.991531, 39.742043"
}

json_add_result_1 = r.json().set("product:46885", Path.root_path(), prd46885)
print(json_add_result_1)  # >>> True

prd46886 = {
    "description": "Bright Green Socks",
    "price": 25.50,
    "city": "Fort Collins",
    "location": "-105.0618814,40.5150098"
}

json_add_result_2 = r.json().set("product:46886", Path.root_path(), prd46886)
print(json_add_result_2)  # >>> True
# STEP_END
# REMOVE_START
assert json_add_result_1
assert json_add_result_2
# REMOVE_END

# STEP_START geo_query
geo_result = r.ft("productidx").search(
    "@location:[-104.800644 38.846127 100 mi]"
)
print(geo_result)
# >>> Result{1 total, docs: [Document {'id': 'product:46885'...
# STEP_END
# REMOVE_START
assert len(geo_result.docs) == 1
assert geo_result.docs[0]["id"] == "product:46885"
# REMOVE_END

# STEP_START create_gshape_idx
geom_schema = (
    TextField("$.name", as_name="name"),
    GeoShapeField(
        "$.geom", as_name="geom", coord_system=GeoShapeField.FLAT
    )
)

geom_index_create_result = r.ft("geomidx").create_index(
    geom_schema,
    definition=IndexDefinition(
        prefix=["shape:"], index_type=IndexType.JSON
    )
)
print(geom_index_create_result)  # True
# STEP_END
# REMOVE_START
assert geom_index_create_result
# REMOVE_END

# STEP_START add_gshape_json
shape1 = {
    "name": "Green Square",
    "geom": "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))"
}

gm_json_res_1 = r.json().set("shape:1", Path.root_path(), shape1)
print(gm_json_res_1)  # >>> True

shape2 = {
    "name": "Red Rectangle",
    "geom": "POLYGON ((2 2.5, 2 3.5, 3.5 3.5, 3.5 2.5, 2 2.5))"
}

gm_json_res_2 = r.json().set("shape:2", Path.root_path(), shape2)
print(gm_json_res_2)  # >>> True

shape3 = {
    "name": "Blue Triangle",
    "geom": "POLYGON ((3.5 1, 3.75 2, 4 1, 3.5 1))"
}

gm_json_res_3 = r.json().set("shape:3", Path.root_path(), shape3)
print(gm_json_res_3)  # >>> True

shape4 = {
    "name": "Purple Point",
    "geom": "POINT (2 2)"
}

gm_json_res_4 = r.json().set("shape:4", Path.root_path(), shape4)
print(gm_json_res_4)  # >>> True
# STEP_END
# REMOVE_START
assert gm_json_res_1
assert gm_json_res_2
assert gm_json_res_3
assert gm_json_res_4
# REMOVE_END

# STEP_START gshape_query
geom_result = r.ft("geomidx").search(
    Query(
        "(-@name:(Green Square) @geom:[WITHIN $qshape])"
    ).dialect(4).paging(0, 1),
    query_params={
        "qshape": "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))"
    }
)
print(geom_result)
# >>> Result{1 total, docs: [Document {'id': 'shape:4'...
# STEP_END
# REMOVE_START
assert len(geom_result.docs) == 1
assert geom_result.docs[0]["id"] == "shape:4"
# REMOVE_END
