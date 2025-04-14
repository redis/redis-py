# EXAMPLE: query_range
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
    TextField("$.description", as_name="description"),
    NumericField("$.price", as_name="price"),
    TagField("$.condition", as_name="condition"),
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

# STEP_START range1
res = index.search(Query("@price:[500 1000]"))
print(res.total)
# >>> 3
# REMOVE_START
assert res.total == 3
# REMOVE_END
# STEP_END

# STEP_START range2
query = Query("*").add_filter(NumericFilter("price", 500, 1000))
res = index.search(query)
print(res.total)
# >>> 3
# REMOVE_START
assert res.total == 3
# REMOVE_END
# STEP_END

# STEP_START range3
query = Query("*").add_filter(NumericFilter("price", "(1000", "+inf"))
res = index.search(query)
print(res.total)
# >>> 5
# REMOVE_START
assert res.total == 5
# REMOVE_END
# STEP_END

# STEP_START range4
query = Query('@price:[-inf 2000]').sort_by('price').paging(0, 5)
res = index.search(query)
print(res.total)
print(res)
# >>> Result{7 total, docs: [Document {'id': 'bicycle:0', ... }, Document {'id': 'bicycle:7', ... }, Document {'id': 'bicycle:5', ... }, ...]
# REMOVE_START
assert res.total == 7
# REMOVE_END
# STEP_END

# REMOVE_START
# destroy index and data
r.ft("idx:bicycle").dropindex(delete_documents=True)
# REMOVE_END
