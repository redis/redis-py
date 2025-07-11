# EXAMPLE: time_series_tutorial
# HIDE_START
"""
Code samples for time series page:
    https://redis.io/docs/latest/develop/data-types/timeseries/
"""

import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# REMOVE_START
r.delete(
    "thermometer:1", "thermometer:2", "thermometer:3",
    "rg:1", "rg:2", "rg:3", "rg:4",
    "sensor3",
    "wind:1", "wind:2", "wind:3", "wind:4",
    "hyg:1", "hyg:compacted"
)
# REMOVE_END

# STEP_START create
res1 = r.ts().create("thermometer:1")
print(res1)  # >>> True

res2 = r.type("thermometer:1")
print(res2)  # >>> TSDB-TYPE

res3 = r.ts().info("thermometer:1")
print(res3)
# >>> {'rules': [], ... 'total_samples': 0, ...
# STEP_END
# REMOVE_START
assert res1 is True
assert res2 == "TSDB-TYPE"
assert res3["total_samples"] == 0
# REMOVE_END

# STEP_START create_retention
res4 = r.ts().add("thermometer:2", 1, 10.8, retention_msecs=100)
print(res4)  # >>> 1

res5 = r.ts().info("thermometer:2")
print(res5)
# >>> {'rules': [], ... 'retention_msecs': 100, ...
# STEP_END
# REMOVE_START
assert res4 == 1
assert res5["retention_msecs"] == 100
# REMOVE_END

# STEP_START create_labels
res6 = r.ts().create(
    "thermometer:3", 1, 10.4,
    labels={"location": "UK", "type": "Mercury"}
)
print(res6)  # >>> 1

res7 = r.ts().info("thermometer:3")
print(res7)
# >>> {'rules': [], ... 'labels': {'location': 'UK', 'type': 'Mercury'}, ...
# STEP_END
# REMOVE_START
assert res6 == 1
assert res7["labels"] == {"location": "UK", "type": "Mercury"}
# REMOVE_END

# STEP_START madd
res8 = r.ts().madd([
    ("thermometer:1", 1, 9.2),
    ("thermometer:1", 2, 9.9),
    ("thermometer:2", 2, 10.3)
])
print(res8)  # >>> [1, 2, 2]
# STEP_END
# REMOVE_START
assert res8 == [1, 2, 2]
# REMOVE_END

# STEP_START get
# The last recorded temperature for thermometer:2
# was 10.3 at time 2.
res9 = r.ts().get("thermometer:2")
print(res9)  # >>> (2, 10.3)
# STEP_END
# REMOVE_START
assert res9 == (2, 10.3)
# REMOVE_END

# STEP_START range
# Add 5 data points to a time series named "rg:1".
res10 = r.ts().create("rg:1")
print(res10)  # >>> True

res11 = r.ts().madd([
        ("rg:1", 0, 18),
        ("rg:1", 1, 14),
        ("rg:1", 2, 22),
        ("rg:1", 3, 18),
        ("rg:1", 4, 24),
])
print(res11)  # >>> [0, 1, 2, 3, 4]

# Retrieve all the data points in ascending order.
res12 = r.ts().range("rg:1", "-", "+")
print(res12)  # >>> [(0, 18.0), (1, 14.0), (2, 22.0), (3, 18.0), (4, 24.0)]

# Retrieve data points up to time 1 (inclusive).
res13 = r.ts().range("rg:1", "-", 1)
print(res13)  # >>> [(0, 18.0), (1, 14.0)]

# Retrieve data points from time 3 onwards.
res14 = r.ts().range("rg:1", 3, "+")
print(res14)  # >>> [(3, 18.0), (4, 24.0)]

# Retrieve all the data points in descending order.
res15 = r.ts().revrange("rg:1", "-", "+")
print(res15)  # >>> [(4, 24.0), (3, 18.0), (2, 22.0), (1, 14.0), (0, 18.0)]

# Retrieve data points up to time 1 (inclusive), but return them
# in descending order.
res16 = r.ts().revrange("rg:1", "-", 1)
print(res16)  # >>> [(1, 14.0), (0, 18.0)]
# STEP_END
# REMOVE_START
assert res10 is True
assert res11 == [0, 1, 2, 3, 4]
assert res12 == [(0, 18.0), (1, 14.0), (2, 22.0), (3, 18.0), (4, 24.0)]
assert res13 == [(0, 18.0), (1, 14.0)]
assert res14 == [(3, 18.0), (4, 24.0)]
assert res15 == [(4, 24.0), (3, 18.0), (2, 22.0), (1, 14.0), (0, 18.0)]
assert res16 == [(1, 14.0), (0, 18.0)]
# REMOVE_END

# STEP_START range_filter
res17 = r.ts().range("rg:1", "-", "+", filter_by_ts=[0, 2, 4])
print(res17)  # >>> [(0, 18.0), (2, 22.0), (4, 24.0)]

res18 = r.ts().revrange(
    "rg:1", "-", "+",
    filter_by_ts=[0, 2, 4],
    filter_by_min_value=20,
    filter_by_max_value=25,
)
print(res18)  # >>> [(4, 24.0), (2, 22.0)]

res19 = r.ts().revrange(
    "rg:1", "-", "+",
    filter_by_ts=[0, 2, 4],
    filter_by_min_value=22,
    filter_by_max_value=22,
    count=1,
)
print(res19)  # >>> [(2, 22.0)]
# STEP_END
# REMOVE_START
assert res17 == [(0, 18.0), (2, 22.0), (4, 24.0)]
assert res18 == [(4, 24.0), (2, 22.0)]
assert res19 == [(2, 22.0)]
# REMOVE_END

# STEP_START query_multi
# Create three new "rg:" time series (two in the US
# and one in the UK, with different units) and add some
# data points.
res20 = r.ts().create(
    "rg:2",
    labels={"location": "us", "unit": "cm"},
)
print(res20)  # >>> True

res21 = r.ts().create(
    "rg:3",
    labels={"location": "us", "unit": "in"},
)
print(res21)  # >>> True

res22 = r.ts().create(
    "rg:4",
    labels={"location": "uk", "unit": "mm"},
)
print(res22)  # >>> True

res23 = r.ts().madd([
        ("rg:2", 0, 1.8),
        ("rg:3", 0, 0.9),
        ("rg:4", 0, 25),
])
print(res23)  # >>> [0, 0, 0]

res24 = r.ts().madd([
        ("rg:2", 1, 2.1),
        ("rg:3", 1, 0.77),
        ("rg:4", 1, 18),
])
print(res24)  # >>> [1, 1, 1]

res25 = r.ts().madd([
        ("rg:2", 2, 2.3),
        ("rg:3", 2, 1.1),
        ("rg:4", 2, 21),
])
print(res25)  # >>> [2, 2, 2]

res26 = r.ts().madd([
        ("rg:2", 3, 1.9),
        ("rg:3", 3, 0.81),
        ("rg:4", 3, 19),
])
print(res26)  # >>> [3, 3, 3]

res27 = r.ts().madd([
        ("rg:2", 4, 1.78),
        ("rg:3", 4, 0.74),
        ("rg:4", 4, 23),
])
print(res27)  # >>> [4, 4, 4]

# Retrieve the last data point from each US time series. If
# you don't specify any labels, an empty array is returned
# for the labels.
res28 = r.ts().mget(["location=us"])
print(res28)  # >>> [{'rg:2': [{}, 4, 1.78]}, {'rg:3': [{}, 4, 0.74]}]

# Retrieve the same data points, but include the `unit`
# label in the results.
res29 = r.ts().mget(["location=us"], select_labels=["unit"])
print(res29)  # >>> [{'unit': 'cm'}, (4, 1.78), {'unit': 'in'}, (4, 0.74)]

# Retrieve data points up to time 2 (inclusive) from all
# time series that use millimeters as the unit. Include all
# labels in the results.
res30 = r.ts().mrange(
    "-", 2, filters=["unit=mm"], with_labels=True
)
print(res30)
# >>> [{'rg:4': [{'location': 'uk', 'unit': 'mm'}, [(0, 25.4),...

# Retrieve data points from time 1 to time 3 (inclusive) from
# all time series that use centimeters or millimeters as the unit,
# but only return the `location` label. Return the results
# in descending order of timestamp.
res31 = r.ts().mrevrange(
    1, 3, filters=["unit=(cm,mm)"], select_labels=["location"]
)
print(res31)
# >>> [[{'location': 'uk'}, (3, 19.0), (2, 21.0), (1, 18.0)],...
# STEP_END
# REMOVE_START
assert res20 is True
assert res21 is True
assert res22 is True
assert res23 == [0, 0, 0]
assert res24 == [1, 1, 1]
assert res25 == [2, 2, 2]
assert res26 == [3, 3, 3]
assert res27 == [4, 4, 4]
assert res28 == [{'rg:2': [{}, 4, 1.78]}, {'rg:3': [{}, 4, 0.74]}]
assert res29 == [
    {'rg:2': [{'unit': 'cm'}, 4, 1.78]},
    {'rg:3': [{'unit': 'in'}, 4, 0.74]}
]
assert res30 == [
    {
        'rg:4': [
            {'location': 'uk', 'unit': 'mm'},
            [(0, 25), (1, 18.0), (2, 21.0)]
        ]
    }
]
assert res31 == [
    {'rg:2': [{'location': 'us'}, [(3, 1.9), (2, 2.3), (1, 2.1)]]},
    {'rg:4': [{'location': 'uk'}, [(3, 19.0), (2, 21.0), (1, 18.0)]]}
]
# REMOVE_END

# STEP_START agg
res32 = r.ts().range(
    "rg:2", "-", "+",
    aggregation_type="avg",
    bucket_size_msec=2
)
print(res32)
# >>> [(0, 1.9500000000000002), (2, 2.0999999999999996), (4, 1.78)]
# STEP_END
# REMOVE_START
assert res32 == [
    (0, 1.9500000000000002), (2, 2.0999999999999996),
    (4, 1.78)
]
# REMOVE_END

# STEP_START agg_bucket
res33 = r.ts().create("sensor3")
print(res33)  # >>> True

res34 = r.ts().madd([
    ("sensor3", 10, 1000),
    ("sensor3", 20, 2000),
    ("sensor3", 30, 3000),
    ("sensor3", 40, 4000),
    ("sensor3", 50, 5000),
    ("sensor3", 60, 6000),
    ("sensor3", 70, 7000),
])
print(res34)  # >>> [10, 20, 30, 40, 50, 60, 70]

res35 = r.ts().range(
    "sensor3", 10, 70,
    aggregation_type="min",
    bucket_size_msec=25
)
print(res35)
# >>> [(0, 1000.0), (25, 3000.0), (50, 5000.0)]
# STEP_END
# REMOVE_START
assert res33 is True
assert res34 == [10, 20, 30, 40, 50, 60, 70]
assert res35 == [(0, 1000.0), (25, 3000.0), (50, 5000.0)]
# REMOVE_END

# STEP_START agg_align
res36 = r.ts().range(
    "sensor3", 10, 70,
    aggregation_type="min",
    bucket_size_msec=25,
    align="START"
)
print(res36)
# >>> [(10, 1000.0), (35, 4000.0), (60, 6000.0)]
# STEP_END
# REMOVE_START
assert res36 == [(10, 1000.0), (35, 4000.0), (60, 6000.0)]
# REMOVE_END

# STEP_START agg_multi
res37 = r.ts().create(
    "wind:1",
    labels={"country": "uk"}
)
print(res37)  # >>> True

res38 = r.ts().create(
    "wind:2",
    labels={"country": "uk"}
)
print(res38)  # >>> True

res39 = r.ts().create(
    "wind:3",
    labels={"country": "us"}
)
print(res39)  # >>> True

res40 = r.ts().create(
    "wind:4",
    labels={"country": "us"}
)
print(res40)  # >>> True

res41 = r.ts().madd([
        ("wind:1", 1, 12),
        ("wind:2", 1, 18),
        ("wind:3", 1, 5),
        ("wind:4", 1, 20),
])
print(res41)  # >>> [1, 1, 1, 1]

res42 = r.ts().madd([
        ("wind:1", 2, 14),
        ("wind:2", 2, 21),
        ("wind:3", 2, 4),
        ("wind:4", 2, 25),
])
print(res42)  # >>> [2, 2, 2, 2]

res43 = r.ts().madd([
        ("wind:1", 3, 10),
        ("wind:2", 3, 24),
        ("wind:3", 3, 8),
        ("wind:4", 3, 18),
])
print(res43)  # >>> [3, 3, 3, 3]

# The result pairs contain the timestamp and the maximum sample value
# for the country at that timestamp.
res44 = r.ts().mrange(
    "-", "+",
    filters=["country=(us,uk)"],
    groupby="country",
    reduce="max"
)
print(res44)
# >>> [{'country=uk': [{}, [(1, 18.0), (2, 21.0), (3, 24.0)]]}, ...

# The result pairs contain the timestamp and the average sample value
# for the country at that timestamp.
res45 = r.ts().mrange(
    "-", "+",
    filters=["country=(us,uk)"],
    groupby="country",
    reduce="avg"
)
print(res45)
# >>> [{'country=uk': [{}, [(1, 15.0), (2, 17.5), (3, 17.0)]]}, ...
# STEP_END
# REMOVE_START
assert res37 is True
assert res38 is True
assert res39 is True
assert res40 is True
assert res41 == [1, 1, 1, 1]
assert res42 == [2, 2, 2, 2]
assert res43 == [3, 3, 3, 3]
assert res44 == [
    {'country=uk': [{}, [(1, 18.0), (2, 21.0), (3, 24.0)]]},
    {'country=us': [{}, [(1, 20.0), (2, 25.0), (3, 18.0)]]}
]
assert res45 == [
    {'country=uk': [{}, [(1, 15.0), (2, 17.5), (3, 17.0)]]},
    {'country=us': [{}, [(1, 12.5), (2, 14.5), (3, 13.0)]]}
]
# REMOVE_END

# STEP_START create_compaction
res45 = r.ts().create("hyg:1")
print(res45)  # >>> True

res46 = r.ts().create("hyg:compacted")
print(res46)  # >>> True

res47 = r.ts().createrule("hyg:1", "hyg:compacted", "min", 3)
print(res47)  # >>> True

res48 = r.ts().info("hyg:1")
print(res48.rules)
# >>> [['hyg:compacted', 3, 'MIN', 0]]

res49 = r.ts().info("hyg:compacted")
print(res49.source_key)  # >>> 'hyg:1'
# STEP_END
# REMOVE_START
assert res45 is True
assert res46 is True
assert res47 is True
assert res48.rules == [['hyg:compacted', 3, 'MIN', 0]]
assert res49.source_key == 'hyg:1'
# REMOVE_END

# STEP_START comp_add
res50 = r.ts().madd([
    ("hyg:1", 0, 75),
    ("hyg:1", 1, 77),
    ("hyg:1", 2, 78),
])
print(res50)  # >>> [0, 1, 2]

res51 = r.ts().range("hyg:compacted", "-", "+")
print(res51)  # >>> []

res52 = r.ts().add("hyg:1", 3, 79)
print(res52)  # >>> 3

res53 = r.ts().range("hyg:compacted", "-", "+")
print(res53)  # >>> [(0, 75.0)]
# STEP_END
# REMOVE_START
assert res50 == [0, 1, 2]
assert res51 == []
assert res52 == 3
assert res53 == [(0, 75.0)]
# REMOVE_END

# STEP_START del
res54 = r.ts().info("thermometer:1")
print(res54.total_samples)  # >>> 2
print(res54.first_timestamp)  # >>> 1
print(res54.last_timestamp)  # >>> 2

res55 = r.ts().add("thermometer:1", 3, 9.7)
print(res55)  # >>> 3

res56 = r.ts().info("thermometer:1")
print(res56.total_samples)  # >>> 3
print(res56.first_timestamp)  # >>> 1
print(res56.last_timestamp)  # >>> 3

res57 = r.ts().delete("thermometer:1", 1, 2)
print(res57)  # >>> 2

res58 = r.ts().info("thermometer:1")
print(res58.total_samples)  # >>> 1
print(res58.first_timestamp)  # >>> 3
print(res58.last_timestamp)  # >>> 3

res59 = r.ts().delete("thermometer:1", 3, 3)
print(res59)  # >>> 1

res60 = r.ts().info("thermometer:1")
print(res60.total_samples)  # >>> 0
# STEP_END
# REMOVE_START
assert res54.total_samples == 2
assert res54.first_timestamp == 1
assert res54.last_timestamp == 2
assert res55 == 3
assert res56.total_samples == 3
assert res56.first_timestamp == 1
assert res56.last_timestamp == 3
assert res57 == 2
assert res58.total_samples == 1
assert res58.first_timestamp == 3
assert res58.last_timestamp == 3
assert res59 == 1
assert res60.total_samples == 0
# REMOVE_END
