# EXAMPLE: geo_tutorial
# HIDE_START
"""
Code samples for Geospatial doc pages:
    https://redis.io/docs/latest/develop/data-types/geospatial/
"""
import redis

r = redis.Redis(decode_responses=True)
# HIDE_END
# REMOVE_START
r.delete("bikes:rentable")
# REMOVE_END

# STEP_START geoadd
res1 = r.geoadd("bikes:rentable", [-122.27652, 37.805186, "station:1"])
print(res1)  # >>> 1

res2 = r.geoadd("bikes:rentable", [-122.2674626, 37.8062344, "station:2"])
print(res2)  # >>> 1

res3 = r.geoadd("bikes:rentable", [-122.2469854, 37.8104049, "station:3"])
print(res3)  # >>> 1
# STEP_END

# REMOVE_START
assert res1 == 1
assert res2 == 1
assert res3 == 1
# REMOVE_END

# STEP_START geosearch
res4 = r.geosearch(
    "bikes:rentable",
    longitude=-122.27652,
    latitude=37.805186,
    radius=5,
    unit="km",
)
print(res4)  # >>> ['station:1', 'station:2', 'station:3']
# STEP_END

# REMOVE_START
assert res4 == ["station:1", "station:2", "station:3"]
# REMOVE_END
