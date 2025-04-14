# EXAMPLE: query_em
# HIDE_START
import json
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

# STEP_START em1
res = index.search(Query("@price:[270 270]"))
print(res.total)
# >>> 1
# REMOVE_START
assert res.total == 1
# REMOVE_END

try:
    res = index.search(Query("@price:[270]")) # not yet supported in redis-py
    print(res.total)
    # >>> 1
    assert res.total == 1
except:
    print("'@price:[270]' syntax not yet supported.")

try:
    res = index.search(Query("@price==270")) # not yet supported in redis-py
    print(res.total)
    # >>> 1
    assert res.total == 1
except:
    print("'@price==270' syntax not yet supported.")

query = Query("*").add_filter(NumericFilter("price", 270, 270))
res = index.search(query)
print(res.total)
# >>> 1
# REMOVE_START
assert res.total == 1
# REMOVE_END
# STEP_END

# STEP_START em2
res = index.search(Query("@condition:{new}"))
print(res.total)
# >>> 5
# REMOVE_START
assert res.total == 5
# REMOVE_END
# STEP_END

# STEP_START em3
schema = (
    TagField("$.email", as_name="email")
)

idx_email = r.ft("idx:email")
idx_email.create_index(
    schema,
    definition=IndexDefinition(prefix=["key:"], index_type=IndexType.JSON),
)
r.json().set('key:1', Path.root_path(), '{"email": "test@redis.com"}')

try:
    res = idx_email.search(Query("test@redis.com").dialect(2))
    print(res)
except:
    print("'test@redis.com' syntax not yet supported.")
# REMOVE_START
r.ft("idx:email").dropindex(delete_documents=True)
# REMOVE_END
# STEP_END

# STEP_START em4
res = index.search(Query("@description:\"rough terrain\""))
print(res.total)
# >>> 1 (Result{1 total, docs: [Document {'id': 'bicycle:8'...)
# REMOVE_START
assert res.total == 1
# REMOVE_END
# STEP_END

# REMOVE_START
# destroy index and data
r.ft("idx:bicycle").dropindex(delete_documents=True)
# REMOVE_END
