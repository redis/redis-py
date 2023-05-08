# EXAMPLE: search_quickstart
# HIDE_START
import redis
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.json.path import Path
from redis.commands.search.field import NumericField, TagField, TextField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

# HIDE_END

# STEP_START connect
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
# STEP_END
# REMOVE_START
try:
    r.ft("idx:bicycle").dropindex()
except Exception:
    pass
# REMOVE_END
# STEP_START data_sample
bicycle1 = {
    "brand": "Diaz Ltd",
    "model": "Dealer Sl",
    "price": 7315.58,
    "description": (
        "The Diaz Ltd Dealer Sl is a reliable choice"
        " for urban cycling. The Diaz Ltd Dealer Sl "
        "is a comfortable choice for urban cycling."
    ),
    "condition": "used",
}
# STEP_END

bicycles = [
    bicycle1,
    {
        "brand": "Bridges Group",
        "model": "Project Pro",
        "price": 3610.82,
        "description": (
            "This mountain bike is perfect for mountain biking. The Bridges"
            " Group Project Pro is a responsive choice for mountain biking."
        ),
        "condition": "used",
    },
    {
        "brand": "Vega, Cole and Miller",
        "model": "Group Advanced",
        "price": 8961.42,
        "description": (
            "The Vega, Cole and Miller Group Advanced provides a excellent"
            " ride. With its fast carbon frame and 24 gears, this bicycle is"
            " perfect for any terrain."
        ),
        "condition": "used",
    },
    {
        "brand": "Powell-Montgomery",
        "model": "Angle Race",
        "price": 4050.27,
        "description": (
            "The Powell-Montgomery Angle Race is a smooth choice for road"
            " cycling. The Powell-Montgomery Angle Race provides a durable"
            " ride."
        ),
        "condition": "used",
    },
    {
        "brand": "Gill-Lewis",
        "model": "Action Evo",
        "price": 283.68,
        "description": (
            "The Gill-Lewis Action Evo provides a smooth ride. The Gill-Lewis"
            " Action Evo provides a excellent ride."
        ),
        "condition": "used",
    },
    {
        "brand": "Rodriguez-Guerrero",
        "model": "Drama Comp",
        "price": 4462.55,
        "description": (
            "This kids bike is perfect for young riders. With its excellent"
            " aluminum frame and 12 gears, this bicycle is perfect for any"
            " terrain."
        ),
        "condition": "new",
    },
    {
        "brand": "Moore PLC",
        "model": "Award Race",
        "price": 3790.76,
        "description": (
            "This olive folding bike features a carbon frame and 27.5 inch"
            " wheels. This folding bike is perfect for compact storage and"
            " transportation."
        ),
        "condition": "new",
    },
    {
        "brand": "Hall, Haley and Hayes",
        "model": "Weekend Plus",
        "price": 2008.4,
        "description": (
            "The Hall, Haley and Hayes Weekend Plus provides a comfortable"
            " ride. This blue kids bike features a steel frame and 29.0 inch"
            " wheels."
        ),
        "condition": "new",
    },
    {
        "brand": "Peck-Carson",
        "model": "Sun Hybrid",
        "price": 9874.95,
        "description": (
            "With its comfortable aluminum frame and 25 gears, this bicycle is"
            " perfect for any terrain. The Peck-Carson Sun Hybrid provides a"
            " comfortable ride."
        ),
        "condition": "new",
    },
    {
        "brand": "Fowler Ltd",
        "model": "Weekend Trail",
        "price": 3833.71,
        "description": (
            "The Fowler Ltd Letter Trail is a comfortable choice for"
            " transporting cargo. This cargo bike is perfect for transporting"
            " cargo."
        ),
        "condition": "refurbished",
    },
]

# STEP_START define_index
schema = (
    TextField("$.brand", as_name="brand"),
    TextField("$.model", as_name="model"),
    TextField("$.description", as_name="description"),
    NumericField("$.price", as_name="price"),
    TagField("$.condition", as_name="condition"),
)
# STEP_END
# STEP_START create_index
index = r.ft("idx:bicycle")
index.create_index(
    schema,
    definition=IndexDefinition(prefix=["bicycle:"], index_type=IndexType.JSON),
)
# STEP_END
# STEP_START add_documents
for bid, bicycle in enumerate(bicycles):
    r.json().set(f"bicycle:{bid}", Path.root_path(), bicycle)
# STEP_END


# STEP_START query_single_term_and_num_range
res = index.search(Query("folding @price:[1000 4000]"))
print(res)
# >>> Result{1 total, docs: [
# Document {
#   'id': 'bicycle:6',
#   'payload': None,
#   'json': '{
#       "brand":"Moore PLC",
#       "model":"Award Race",
#       "price":3790.76,
#       "description":"This olive folding bike features a carbon frame and
#       27.5-inch wheels. This folding bike is perfect
#       for compact storage and transportation."
#   }'
# }]}
# STEP_END
# REMOVE_START
assert res.docs[0].id == "bicycle:6"
# REMOVE_END

# STEP_START query_single_term_limit_fields
res = index.search(Query("cargo").return_field("$.price", as_field="price"))
print(res)
# >>> [Document {'id': 'bicycle:9', 'payload': None, 'price': '3833.71'}]
# STEP_END
# REMOVE_START
assert res.docs[0].id == "bicycle:9"
# REMOVE_END

# STEP_START simple_aggregation
req = aggregations.AggregateRequest("*").group_by(
    "@condition", reducers.count().alias("count")
)
res = index.aggregate(req).rows
print(res)
# >>> [['condition', 'refurbished', 'count', '1'],
#      ['condition', 'used', 'count', '5'],
#      ['condition', 'new', 'count', '4']]
# STEP_END
# REMOVE_START
assert len(res) == 3
# REMOVE_END
