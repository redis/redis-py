# EXAMPLE: query_agg
# HIDE_START
import json
import redis
from redis.commands.json.path import Path
from redis.commands.search import Search
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search.field import NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
import redis.commands.search.reducers as reducers

r = redis.Redis(decode_responses=True)

# create index
schema = (
    TagField("$.condition", as_name="condition"),
    NumericField("$.price", as_name="price"),
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

# STEP_START agg1
search = Search(r, index_name="idx:bicycle")
aggregate_request = AggregateRequest(query='@condition:{new}') \
    .load('__key', 'price') \
    .apply(discounted='@price - (@price * 0.1)')
res = search.aggregate(aggregate_request)
print(len(res.rows)) # >>> 5
print(res.rows) # >>> [['__key', 'bicycle:0', ...
#[['__key', 'bicycle:0', 'price', '270', 'discounted', '243'],
# ['__key', 'bicycle:5', 'price', '810', 'discounted', '729'],
# ['__key', 'bicycle:6', 'price', '2300', 'discounted', '2070'],
# ['__key', 'bicycle:7', 'price', '430', 'discounted', '387'],
# ['__key', 'bicycle:8', 'price', '1200', 'discounted', '1080']]
# REMOVE_START
assert len(res.rows) == 5
# REMOVE_END
# STEP_END

# STEP_START agg2
search = Search(r, index_name="idx:bicycle")
aggregate_request = AggregateRequest(query='*') \
    .load('price') \
    .apply(price_category='@price<1000') \
    .group_by('@condition', reducers.sum('@price_category').alias('num_affordable'))
res = search.aggregate(aggregate_request)
print(len(res.rows)) # >>> 3
print(res.rows) # >>>
#[['condition', 'refurbished', 'num_affordable', '1'],
# ['condition', 'used', 'num_affordable', '1'],
# ['condition', 'new', 'num_affordable', '3']]
# REMOVE_START
assert len(res.rows) == 3
# REMOVE_END
# STEP_END

# STEP_START agg3
search = Search(r, index_name="idx:bicycle")
aggregate_request = AggregateRequest(query='*') \
    .apply(type="'bicycle'") \
    .group_by('@type', reducers.count().alias('num_total'))
res = search.aggregate(aggregate_request)
print(len(res.rows)) # >>> 1
print(res.rows) # >>> [['type', 'bicycle', 'num_total', '10']]
# REMOVE_START
assert len(res.rows) == 1
# REMOVE_END
# STEP_END

# STEP_START agg4
search = Search(r, index_name="idx:bicycle")
aggregate_request = AggregateRequest(query='*') \
    .load('__key') \
    .group_by('@condition', reducers.tolist('__key').alias('bicycles'))
res = search.aggregate(aggregate_request)
print(len(res.rows)) # >>> 3
print(res.rows) # >>>
#[['condition', 'refurbished', 'bicycles', ['bicycle:9']],
# ['condition', 'used', 'bicycles', ['bicycle:1', 'bicycle:2', 'bicycle:3', 'bicycle:4']],
# ['condition', 'new', 'bicycles', ['bicycle:5', 'bicycle:6', 'bicycle:7', 'bicycle:0', 'bicycle:8']]]
# REMOVE_START
assert len(res.rows) == 3
# REMOVE_END
# STEP_END

# REMOVE_START
# destroy index and data
r.ft("idx:bicycle").dropindex(delete_documents=True)
# REMOVE_END
