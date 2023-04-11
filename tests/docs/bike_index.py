# EXAMPLE: bike_index
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

bikes = [{"model": "Ariel", "brand": "Velorim", "price": 2098, "type": "Road bikes", "specs": {"material": "carbon", "weight": 10.9}, "description": "The bike has a lightweight form factor, making it easier for seniors to use. The hydraulic disc brakes provide powerful and modulated braking even in wet conditions, whilst the 3x8 drivetrain offers a huge choice of gears. It's for the rider who wants both efficiency and capability."},{"model": "Makemake", "brand": "Breakout", "price": 4551, "type": "Commuter bikes", "specs": {"material": "carbon", "weight": 12.9}, "description": "The perfect commuter bike for anyone who is constantly rushing around, and prone to forgetting to charge lights, maintain their bike, or not quite getting round to checking weather reports. The Shimano Claris 8-speed groupset gives plenty of gear range to tackle hills and there's room for mudguards and a rack too.  Put it all together and you get a bike that helps redefine what can be done for this price."},{"model": "Iapetus", "brand": "BikeShind", "price": 2637, "type": "Mountain bikes", "specs": {"material": "aluminium", "weight": 7.3}, "description": "This bike fills the space between a pure XC race bike, and a trail bike. It is light, with shorter travel (115mm rear and 120mm front), and quick handling. With a slick-shifting Claris gears from Shimano's, this is a bike which doesn't break the bank and delivers craved performance.  It's for the rider who wants both efficiency and capability."},{"model": "Tethys", "brand": "Classic wheels", "price": 2018, "type": "Kids mountain bikes", "specs": {"material": "carbon", "weight": 15.7}, "description": "Small and powerful, this bike is the best ride for the smallest of tikes. At this price point, you get a Shimano 105 hydraulic groupset with a RS510 crank set. The wheels have had a slight upgrade for 2022, so you're now getting DT Swiss R470 rims with the Formula hubs. That said, we feel this bike is a fantastic option for the rider seeking the versatility that this highly adjustable bike provides."},{"model": "Tethys", "brand": "ScramBikes", "price": 3344, "type": "Kids mountain bikes", "specs": {"material": "carbon", "weight": 14.7}, "description": "Kids want to ride with as little weight as possible. Especially on an incline! With a slick-shifting Claris gears from Shimano's, this is a bike which doesn't break the bank and delivers craved performance.  If you're after a budget option, this is one of the best bikes you could get."},{"model": "Deimos", "brand": "Bold bicycles", "price": 2652, "type": "Commuter bikes", "specs": {"material": "aluminium", "weight": 13.7}, "description": "This bike is the perfect commuting companion for anyone just looking to get the job done With a slick-shifting Claris gears from Shimano's, this is a bike which doesn't break the bank and delivers craved performance.  Put it all together and you get a bike that helps redefine what can be done for this price."},{"model": "Iapetus", "brand": "ScramBikes", "price": 2707, "type": "Enduro bikes", "specs": {"material": "full-carbon", "weight": 11.8}, "description": "The new version with 142mm rear, 160mm front travel is longer and slacker than its previous generation, but it's also a bit taller and steeper than much of its competition. With a slick-shifting Claris gears from Shimano's, this is a bike which doesn't break the bank and delivers craved performance.  It comes fully assembled (no convoluted instructions!) and includes a sturdy helmet at no cost."},{"model": "Mars", "brand": "7th Generation", "price": 2206, "type": "Kids mountain bikes", "specs": {"material": "aluminium", "weight": 8.9}, "description": "This bike is an entry-level kids mountain bike that is a good choice if your MTB enthusiast is just taking to the trails and wants good suspension and easy gearing, without the cost of some more expensive models. The Shimano Claris 8-speed groupset gives plenty of gear range to tackle hills and there's room for mudguards and a rack too.  All bikes are great in their own way, but this bike will be one of the best you've ridden."},{"model": "Millenium-falcon", "brand": "Breakout", "price": 826, "type": "Mountain bikes", "specs": {"material": "aluminium", "weight": 9.0}, "description": "This bike fills the space between a pure XC race bike, and a trail bike. It is light, with shorter travel (115mm rear and 120mm front), and quick handling. The Shimano gear system effectively does away with an external cassette, so is super low maintenance in terms of wear and tear. This is the bike for the rider who wants trail manners with low fuss ownership."}]

count = 0
for bike in bikes:
    r.json().set(f"bikes:{count}", "$", bike)
    count += 1

# define the fields for a bike
bike_schema = [
        TextField("$.model", as_name="model", sortable=True, no_stem=True),
        TextField("$.brand", as_name="brand", sortable=True, no_stem=True),
        NumericField("$.price", as_name="price", sortable=True),
        TagField("$.type", as_name="type"),
        TagField("$.specs.material", as_name="material"),
        NumericField("$.specs.weight", as_name="weight", sortable=True),
        TextField("$.description", as_name="description")
    ]

# define index information
schema_info = IndexDefinition(
        index_type=IndexType.JSON, 
        prefix=["bikes:"])

# create the index
r.ft("idx:bikes").create_index(bike_schema,definition=schema_info)