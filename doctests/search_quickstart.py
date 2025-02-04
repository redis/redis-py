# EXAMPLE: search_quickstart
# HIDE_START
"""
Code samples for document database quickstart pages:
    https://redis.io/docs/latest/develop/get-started/document-database/
"""

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
bicycle = {
    "brand": "Velorim",
    "model": "Jigger",
    "price": 270,
    "description": (
        "Small and powerful, the Jigger is the best ride "
        "for the smallest of tikes! This is the tiniest "
        "kids’ pedal bike on the market available without"
        " a coaster brake, the Jigger is the vehicle of "
        "choice for the rare tenacious little rider "
        "raring to go."
    ),
    "condition": "new",
}
# STEP_END

bicycles = [
    bicycle,
    {
        "brand": "Bicyk",
        "model": "Hillcraft",
        "price": 1200,
        "description": (
            "Kids want to ride with as little weight as possible."
            " Especially on an incline! They may be at the age "
            'when a 27.5" wheel bike is just too clumsy coming '
            'off a 24" bike. The Hillcraft 26 is just the solution'
            " they need!"
        ),
        "condition": "used",
    },
    {
        "brand": "Nord",
        "model": "Chook air 5",
        "price": 815,
        "description": (
            "The Chook Air 5  gives kids aged six years and older "
            "a durable and uberlight mountain bike for their first"
            " experience on tracks and easy cruising through forests"
            " and fields. The lower  top tube makes it easy to mount"
            " and dismount in any situation, giving your kids greater"
            " safety on the trails."
        ),
        "condition": "used",
    },
    {
        "brand": "Eva",
        "model": "Eva 291",
        "price": 3400,
        "description": (
            "The sister company to Nord, Eva launched in 2005 as the"
            " first and only women-dedicated bicycle brand. Designed"
            " by women for women, allEva bikes are optimized for the"
            " feminine physique using analytics from a body metrics"
            " database. If you like 29ers, try the Eva 291. It’s a "
            "brand new bike for 2022.. This full-suspension, "
            "cross-country ride has been designed for velocity. The"
            " 291 has 100mm of front and rear travel, a superlight "
            "aluminum frame and fast-rolling 29-inch wheels. Yippee!"
        ),
        "condition": "used",
    },
    {
        "brand": "Noka Bikes",
        "model": "Kahuna",
        "price": 3200,
        "description": (
            "Whether you want to try your hand at XC racing or are "
            "looking for a lively trail bike that's just as inspiring"
            " on the climbs as it is over rougher ground, the Wilder"
            " is one heck of a bike built specifically for short women."
            " Both the frames and components have been tweaked to "
            "include a women’s saddle, different bars and unique "
            "colourway."
        ),
        "condition": "used",
    },
    {
        "brand": "Breakout",
        "model": "XBN 2.1 Alloy",
        "price": 810,
        "description": (
            "The XBN 2.1 Alloy is our entry-level road bike – but that’s"
            " not to say that it’s a basic machine. With an internal "
            "weld aluminium frame, a full carbon fork, and the slick-shifting"
            " Claris gears from Shimano’s, this is a bike which doesn’t"
            " break the bank and delivers craved performance."
        ),
        "condition": "new",
    },
    {
        "brand": "ScramBikes",
        "model": "WattBike",
        "price": 2300,
        "description": (
            "The WattBike is the best e-bike for people who still feel young"
            " at heart. It has a Bafang 1000W mid-drive system and a 48V"
            " 17.5AH Samsung Lithium-Ion battery, allowing you to ride for"
            " more than 60 miles on one charge. It’s great for tackling hilly"
            " terrain or if you just fancy a more leisurely ride. With three"
            " working modes, you can choose between E-bike, assisted bicycle,"
            " and normal bike modes."
        ),
        "condition": "new",
    },
    {
        "brand": "Peaknetic",
        "model": "Secto",
        "price": 430,
        "description": (
            "If you struggle with stiff fingers or a kinked neck or back after"
            " a few minutes on the road, this lightweight, aluminum bike"
            " alleviates those issues and allows you to enjoy the ride. From"
            " the ergonomic grips to the lumbar-supporting seat position, the"
            " Roll Low-Entry offers incredible comfort. The rear-inclined seat"
            " tube facilitates stability by allowing you to put a foot on the"
            " ground to balance at a stop, and the low step-over frame makes it"
            " accessible for all ability and mobility levels. The saddle is"
            " very soft, with a wide back to support your hip joints and a"
            " cutout in the center to redistribute that pressure. Rim brakes"
            " deliver satisfactory braking control, and the wide tires provide"
            " a smooth, stable ride on paved roads and gravel. Rack and fender"
            " mounts facilitate setting up the Roll Low-Entry as your preferred"
            " commuter, and the BMX-like handlebar offers space for mounting a"
            " flashlight, bell, or phone holder."
        ),
        "condition": "new",
    },
    {
        "brand": "nHill",
        "model": "Summit",
        "price": 1200,
        "description": (
            "This budget mountain bike from nHill performs well both on bike"
            " paths and on the trail. The fork with 100mm of travel absorbs"
            " rough terrain. Fat Kenda Booster tires give you grip in corners"
            " and on wet trails. The Shimano Tourney drivetrain offered enough"
            " gears for finding a comfortable pace to ride uphill, and the"
            " Tektro hydraulic disc brakes break smoothly. Whether you want an"
            " affordable bike that you can take to work, but also take trail in"
            " mountains on the weekends or you’re just after a stable,"
            " comfortable ride for the bike path, the Summit gives a good value"
            " for money."
        ),
        "condition": "new",
    },
    {
        "model": "ThrillCycle",
        "brand": "BikeShind",
        "price": 815,
        "description": (
            "An artsy,  retro-inspired bicycle that’s as functional as it is"
            " pretty: The ThrillCycle steel frame offers a smooth ride. A"
            " 9-speed drivetrain has enough gears for coasting in the city, but"
            " we wouldn’t suggest taking it to the mountains. Fenders protect"
            " you from mud, and a rear basket lets you transport groceries,"
            " flowers and books. The ThrillCycle comes with a limited lifetime"
            " warranty, so this little guy will last you long past graduation."
        ),
        "condition": "refurbished",
    },
]

# STEP_START create_index
schema = (
    TextField("$.brand", as_name="brand"),
    TextField("$.model", as_name="model"),
    TextField("$.description", as_name="description"),
    NumericField("$.price", as_name="price"),
    TagField("$.condition", as_name="condition"),
)

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


# STEP_START wildcard_query
res = index.search(Query("*"))
print("Documents found:", res.total)
# >>> Documents found: 10
# STEP_END
# REMOVE_START
assert res.total == 10
# REMOVE_END

# STEP_START query_single_term
res = index.search(Query("@model:Jigger"))
print(res)
# >>> Result{1 total, docs: [
# Document {
#   'id': 'bicycle:0',
#   'payload': None,
#   'json': '{
#       "brand":"Velorim",
#       "model":"Jigger",
#       "price":270,
#       ...
#       "condition":"new"
#    }'
# }]}
# STEP_END
# REMOVE_START
assert res.docs[0].id == "bicycle:0"
# REMOVE_END

# STEP_START query_single_term_limit_fields
res = index.search(Query("@model:Jigger").return_field("$.price", as_field="price"))
print(res)
# >>> [Document {'id': 'bicycle:0', 'payload': None, 'price': '270'}]
# STEP_END
# REMOVE_START
assert res.docs[0].id == "bicycle:0"
# REMOVE_END

# STEP_START query_single_term_and_num_range
res = index.search(Query("basic @price:[500 1000]"))
print(res)
# >>> Result{1 total, docs: [
# Document {
#   'id': 'bicycle:5',
#   'payload': None,
#   'json': '{
#       "brand":"Breakout",
#       "model":"XBN 2.1 Alloy",
#       "price":810,
#       ...
#       "condition":"new"
#    }'
# }]}
# STEP_END
# REMOVE_START
assert res.docs[0].id == "bicycle:5"
# REMOVE_END

# STEP_START query_exact_matching
res = index.search(Query('@brand:"Noka Bikes"'))
print(res)
# >>> Result{1 total, docs: [
# Document {
#   'id': 'bicycle:4',
#   'payload': None,
#   'json': '{
#       "brand":"Noka Bikes",
#       "model":"Kahuna",
#       "price":3200,
#       ...
#       "condition":"used"
#    }'
# }]}
# STEP_END
# REMOVE_START
assert res.docs[0].id == "bicycle:4"
# REMOVE_END

# STEP_START query_fuzzy_matching
res = index.search(
    Query("@description:%analitics%").dialect(  # Note the typo in the word "analytics"
        2
    )
)
print(res)
# >>> Result{1 total, docs: [
# Document {
#   'id': 'bicycle:3',
#   'payload': None,
#   'json': '{
#       "brand":"Eva",
#       "model":"Eva 291",
#       "price":3400,
#       "description":"...using analytics from a body metrics database...",
#       "condition":"used"
#    }'
# }]}
# STEP_END
# REMOVE_START
assert res.docs[0].id == "bicycle:3"
# REMOVE_END

# STEP_START query_fuzzy_matching_level2
res = index.search(
    Query("@description:%%analitycs%%").dialect(  # Note 2 typos in the word "analytics"
        2
    )
)
print(res)
# >>> Result{1 total, docs: [
# Document {
#   'id': 'bicycle:3',
#   'payload': None,
#   'json': '{
#       "brand":"Eva",
#       "model":"Eva 291",
#       "price":3400,
#       "description":"...using analytics from a body metrics database...",
#       "condition":"used"
#    }'
# }]}
# STEP_END
# REMOVE_START
assert res.docs[0].id == "bicycle:3"
# REMOVE_END

# STEP_START query_prefix_matching
res = index.search(Query("@model:hill*"))
print(res)
# >>> Result{1 total, docs: [
# Document {
#   'id': 'bicycle:1',
#   'payload': None,
#   'json': '{
#       "brand":"Bicyk",
#       "model":"Hillcraft",
#       "price":1200,
#       ...
#       "condition":"used"
#    }'
# }]}
# STEP_END
# REMOVE_START
assert res.docs[0].id == "bicycle:1"
# REMOVE_END

# STEP_START query_suffix_matching
res = index.search(Query("@model:*bike"))
print(res)
# >>> Result{1 total, docs: [
# Document {
#   'id': 'bicycle:6',
#   'payload': None,
#   'json': '{
#       "brand":"ScramBikes",
#       "model":"WattBike",
#       "price":2300,
#       ...
#       "condition":"new"
#   }'
# }]}
# STEP_END
# REMOVE_START
assert res.docs[0].id == "bicycle:6"
# REMOVE_END

# STEP_START query_wildcard_matching
res = index.search(Query("w'H?*craft'").dialect(2))
print(res.docs[0].json)
# >>> {
#   "brand":"Bicyk",
#   "model":"Hillcraft",
#   "price":1200,
#   ...
#   "condition":"used"
# }
# STEP_END
# REMOVE_START
assert res.docs[0].id == "bicycle:1"
# REMOVE_END


# STEP_START query_with_default_scorer
res = index.search(Query("mountain").with_scores())
for sr in res.docs:
    print(f"{sr.id}: score={sr.score}")
# STEP_END
# REMOVE_START
assert res.total == 3
# REMOVE_END

# STEP_START query_with_bm25_scorer
res = index.search(Query("mountain").with_scores().scorer("BM25"))
for sr in res.docs:
    print(f"{sr.id}: score={sr.score}")
# STEP_END
# REMOVE_START
assert res.total == 3
assert res.docs[0].score == res.docs[1].score
# REMOVE_END

# STEP_START simple_aggregation
req = aggregations.AggregateRequest("*").group_by(
    "@condition", reducers.count().alias("count")
)
res = index.aggregate(req).rows
print(res)
# >>> [['condition', 'refurbished', 'count', '1'],
#      ['condition', 'used', 'count', '4'],
#      ['condition', 'new', 'count', '5']]
# STEP_END
# REMOVE_START
assert len(res) == 3
# REMOVE_END
