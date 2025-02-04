# EXAMPLE: query_combined
# HIDE_START
import json
import numpy as np
import redis
import warnings
from redis.commands.json.path import Path
from redis.commands.search.field import NumericField, TagField, TextField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from sentence_transformers import  SentenceTransformer


def embed_text(model, text):
    return np.array(model.encode(text)).astype(np.float32).tobytes()

warnings.filterwarnings("ignore", category=FutureWarning, message=r".*clean_up_tokenization_spaces.*")
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
query = "Bike for small kids"
query_vector = embed_text(model, query)

r = redis.Redis(decode_responses=True)

# create index
schema = (
    TextField("$.description", no_stem=True, as_name="model"),
    TagField("$.condition", as_name="condition"),
    NumericField("$.price", as_name="price"),
    VectorField(
        "$.description_embeddings",
        "FLAT",
        {
            "TYPE": "FLOAT32",
            "DIM": 384,
            "DISTANCE_METRIC": "COSINE",
        },
        as_name="vector",
    ),
)

index = r.ft("idx:bicycle")
index.create_index(
    schema,
    definition=IndexDefinition(prefix=["bicycle:"], index_type=IndexType.JSON),
)

# load data
with open("data/query_vector.json") as f:
    bicycles = json.load(f)

pipeline = r.pipeline(transaction=False)
for bid, bicycle in enumerate(bicycles):
    pipeline.json().set(f'bicycle:{bid}', Path.root_path(), bicycle)
pipeline.execute()
# HIDE_END

# STEP_START combined1
q = Query("@price:[500 1000] @condition:{new}")
res = index.search(q)
print(res.total) # >>> 1
# REMOVE_START
assert res.total == 1
# REMOVE_END
# STEP_END

# STEP_START combined2
q = Query("kids @price:[500 1000] @condition:{used}")
res = index.search(q)
print(res.total) # >>> 1
# REMOVE_START
assert res.total == 1
# REMOVE_END
# STEP_END

# STEP_START combined3
q = Query("(kids | small) @condition:{used}")
res = index.search(q)
print(res.total) # >>> 2
# REMOVE_START
assert res.total == 2
# REMOVE_END
# STEP_END

# STEP_START combined4
q = Query("@description:(kids | small) @condition:{used}")
res = index.search(q)
print(res.total) # >>> 0
# REMOVE_START
assert res.total == 0
# REMOVE_END
# STEP_END

# STEP_START combined5
q = Query("@description:(kids | small) @condition:{new | used}")
res = index.search(q)
print(res.total) # >>> 0
# REMOVE_START
assert res.total == 0
# REMOVE_END
# STEP_END

# STEP_START combined6
q = Query("@price:[500 1000] -@condition:{new}")
res = index.search(q)
print(res.total) # >>> 2
# REMOVE_START
assert res.total == 2
# REMOVE_END
# STEP_END

# STEP_START combined7
q = Query("(@price:[500 1000] -@condition:{new})=>[KNN 3 @vector $query_vector]").dialect(2)
# put query string here
res = index.search(q,{ 'query_vector': query_vector })
print(res.total) # >>> 2
# REMOVE_START
assert res.total == 2
# REMOVE_END
# STEP_END

# REMOVE_START
# destroy index and data
r.ft("idx:bicycle").dropindex(delete_documents=True)
# REMOVE_END
