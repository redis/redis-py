# EXAMPLE: bike_setup
import redis
import json
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.field import TextField, TagField, NumericField
from redis.commands.search.query import Query
from redis.commands.search.aggregation import AggregateRequest, Desc, Asc
import redis.commands.search.reducers as reducers

# define a Redis connection
r = redis.Redis(
    host="localhost", 
    port=6379, 
    db=0, 
    decode_responses=True
)

# a single bike to demonstrate the basics
bike1 = {
    "model": "Hyperion", 
    "brand": "Velorim", 
    "price": 844, 
    "type": "Enduro bikes", 
    "specs": {
        "material": "full-carbon", 
        "weight": 8.7
    }, 
    "description": "This is a mid-travel trail slayer that is a fantastic daily driver or one bike quiver. At this price point, you get a Shimano 105 hydraulic groupset with a RS510 crank set. The wheels have had a slight upgrade for 2022, so you\"re now getting DT Swiss R470 rims with the Formula hubs. Put it all together and you get a bike that helps redefine what can be done for this price."
}

r.json().set("bikes:1", "$", bike1)

r.json().get("bikes:1", "$.model")
r.json().get("bikes:1", "$.specs.material")

r.json().set("bikes:1", "$.model", "Hyperion1")
r.json().get("bikes:1", "$.model")