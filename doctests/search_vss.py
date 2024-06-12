# EXAMPLE: search_vss
# HIDE_START
"""
Code samples for vector database quickstart pages:
    https://redis.io/docs/latest/develop/get-started/vector-database/
"""
# HIDE_END

# STEP_START imports
import json
import time

import numpy as np
import pandas as pd
import requests
import redis
from redis.commands.search.field import (
    NumericField,
    TagField,
    TextField,
    VectorField,
)
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from sentence_transformers import SentenceTransformer

# STEP_END

# STEP_START get_data
URL = ("https://raw.githubusercontent.com/bsbodden/redis_vss_getting_started"
       "/main/data/bikes.json"
       )
response = requests.get(URL, timeout=10)
bikes = response.json()
# STEP_END
# REMOVE_START
assert bikes[0]["model"] == "Jigger"
# REMOVE_END

# STEP_START dump_data
json.dumps(bikes[0], indent=2)
# STEP_END

# STEP_START connect
client = redis.Redis(host="localhost", port=6379, decode_responses=True)
# STEP_END

# STEP_START connection_test
res = client.ping()
# >>> True
# STEP_END
# REMOVE_START
assert res
# REMOVE_END

# STEP_START load_data
pipeline = client.pipeline()
for i, bike in enumerate(bikes, start=1):
    redis_key = f"bikes:{i:03}"
    pipeline.json().set(redis_key, "$", bike)
res = pipeline.execute()
# >>> [True, True, True, True, True, True, True, True, True, True, True]
# STEP_END
# REMOVE_START
assert res == [True, True, True, True, True, True, True, True, True, True, True]
# REMOVE_END

# STEP_START get
res = client.json().get("bikes:010", "$.model")
# >>> ['Summit']
# STEP_END
# REMOVE_START
assert res == ["Summit"]
# REMOVE_END

# STEP_START get_keys
keys = sorted(client.keys("bikes:*"))
# >>> ['bikes:001', 'bikes:002', ..., 'bikes:011']
# STEP_END
# REMOVE_START
assert keys[0] == "bikes:001"
# REMOVE_END

# STEP_START generate_embeddings
descriptions = client.json().mget(keys, "$.description")
descriptions = [item for sublist in descriptions for item in sublist]
embedder = SentenceTransformer("msmarco-distilbert-base-v4")
embeddings = embedder.encode(descriptions).astype(np.float32).tolist()
VECTOR_DIMENSION = len(embeddings[0])
# >>> 768
# STEP_END
# REMOVE_START
assert VECTOR_DIMENSION == 768
# REMOVE_END

# STEP_START load_embeddings
pipeline = client.pipeline()
for key, embedding in zip(keys, embeddings):
    pipeline.json().set(key, "$.description_embeddings", embedding)
pipeline.execute()
# >>> [True, True, True, True, True, True, True, True, True, True, True]
# STEP_END

# STEP_START dump_example
res = client.json().get("bikes:010")
# >>>
# {
#   "model": "Summit",
#   "brand": "nHill",
#   "price": 1200,
#   "type": "Mountain Bike",
#   "specs": {
#     "material": "alloy",
#     "weight": "11.3"
#   },
#   "description": "This budget mountain bike from nHill performs well..."
#   "description_embeddings": [
#     -0.538114607334137,
#     -0.49465855956077576,
#     -0.025176964700222015,
#     ...
#   ]
# }
# STEP_END
# REMOVE_START
assert len(res["description_embeddings"]) == 768
# REMOVE_END

# STEP_START create_index
schema = (
    TextField("$.model", no_stem=True, as_name="model"),
    TextField("$.brand", no_stem=True, as_name="brand"),
    NumericField("$.price", as_name="price"),
    TagField("$.type", as_name="type"),
    TextField("$.description", as_name="description"),
    VectorField(
        "$.description_embeddings",
        "FLAT",
        {
            "TYPE": "FLOAT32",
            "DIM": VECTOR_DIMENSION,
            "DISTANCE_METRIC": "COSINE",
        },
        as_name="vector",
    ),
)
definition = IndexDefinition(prefix=["bikes:"], index_type=IndexType.JSON)
res = client.ft("idx:bikes_vss").create_index(fields=schema, definition=definition)
# >>> 'OK'
# STEP_END
# REMOVE_START
assert res == "OK"
time.sleep(2)
# REMOVE_END

# STEP_START validate_index
info = client.ft("idx:bikes_vss").info()
num_docs = info["num_docs"]
indexing_failures = info["hash_indexing_failures"]
# print(f"{num_docs} documents indexed with {indexing_failures} failures")
# >>> 11 documents indexed with 0 failures
# STEP_END
# REMOVE_START
assert (num_docs == "11") and (indexing_failures == "0")
# REMOVE_END

# STEP_START simple_query_1
query = Query("@brand:Peaknetic")
res = client.ft("idx:bikes_vss").search(query).docs
# print(res)
# >>> [
#       Document {
#           'id': 'bikes:008',
#           'payload': None,
#           'brand': 'Peaknetic',
#           'model': 'Soothe Electric bike',
#           'price': '1950', 'description_embeddings': ...
# STEP_END
# REMOVE_START

assert all(
    item in [x.__dict__["id"] for x in res] for item in ["bikes:008", "bikes:009"]
)
# REMOVE_END

# STEP_START simple_query_2
query = Query("@brand:Peaknetic").return_fields("id", "brand", "model", "price")
res = client.ft("idx:bikes_vss").search(query).docs
# print(res)
# >>> [
#       Document {
#           'id': 'bikes:008',
#           'payload': None,
#           'brand': 'Peaknetic',
#           'model': 'Soothe Electric bike',
#           'price': '1950'
#       },
#       Document {
#           'id': 'bikes:009',
#           'payload': None,
#           'brand': 'Peaknetic',
#           'model': 'Secto',
#           'price': '430'
#       }
# ]
# STEP_END
# REMOVE_START
assert all(
    item in [x.__dict__["id"] for x in res] for item in ["bikes:008", "bikes:009"]
)
# REMOVE_END

# STEP_START simple_query_3
query = Query("@brand:Peaknetic @price:[0 1000]").return_fields(
    "id", "brand", "model", "price"
)
res = client.ft("idx:bikes_vss").search(query).docs
# print(res)
# >>> [
#       Document {
#           'id': 'bikes:009',
#           'payload': None,
#           'brand': 'Peaknetic',
#           'model': 'Secto',
#           'price': '430'
#       }
# ]
# STEP_END
# REMOVE_START
assert all(item in [x.__dict__["id"] for x in res] for item in ["bikes:009"])
# REMOVE_END

# STEP_START def_bulk_queries
queries = [
    "Bike for small kids",
    "Best Mountain bikes for kids",
    "Cheap Mountain bike for kids",
    "Female specific mountain bike",
    "Road bike for beginners",
    "Commuter bike for people over 60",
    "Comfortable commuter bike",
    "Good bike for college students",
    "Mountain bike for beginners",
    "Vintage bike",
    "Comfortable city bike",
]
# STEP_END

# STEP_START enc_bulk_queries
encoded_queries = embedder.encode(queries)
len(encoded_queries)
# >>> 11
# STEP_END
# REMOVE_START
assert len(encoded_queries) == 11
# REMOVE_END


# STEP_START define_bulk_query
def create_query_table(query, queries, encoded_queries, extra_params=None):
    """
    Creates a query table.
    """
    results_list = []
    for i, encoded_query in enumerate(encoded_queries):
        result_docs = (
            client.ft("idx:bikes_vss")
            .search(
                query,
                {"query_vector": np.array(encoded_query, dtype=np.float32).tobytes()}
                | (extra_params if extra_params else {}),
            )
            .docs
        )
        for doc in result_docs:
            vector_score = round(1 - float(doc.vector_score), 2)
            results_list.append(
                {
                    "query": queries[i],
                    "score": vector_score,
                    "id": doc.id,
                    "brand": doc.brand,
                    "model": doc.model,
                    "description": doc.description,
                }
            )

    # Optional: convert the table to Markdown using Pandas
    queries_table = pd.DataFrame(results_list)
    queries_table.sort_values(
        by=["query", "score"], ascending=[True, False], inplace=True
    )
    queries_table["query"] = queries_table.groupby("query")["query"].transform(
        lambda x: [x.iloc[0]] + [""] * (len(x) - 1)
    )
    queries_table["description"] = queries_table["description"].apply(
        lambda x: (x[:497] + "...") if len(x) > 500 else x
    )
    return queries_table.to_markdown(index=False)


# STEP_END

# STEP_START run_knn_query
query = (
    Query("(*)=>[KNN 3 @vector $query_vector AS vector_score]")
    .sort_by("vector_score")
    .return_fields("vector_score", "id", "brand", "model", "description")
    .dialect(2)
)

table = create_query_table(query, queries, encoded_queries)
print(table)
# >>> | Best Mountain bikes for kids     |    0.54 | bikes:003...
# STEP_END

# STEP_START run_hybrid_query
hybrid_query = (
    Query("(@brand:Peaknetic)=>[KNN 3 @vector $query_vector AS vector_score]")
    .sort_by("vector_score")
    .return_fields("vector_score", "id", "brand", "model", "description")
    .dialect(2)
)
table = create_query_table(hybrid_query, queries, encoded_queries)
print(table)
# >>> | Best Mountain bikes for kids     |    0.3  | bikes:008...
# STEP_END

# STEP_START run_range_query
range_query = (
    Query(
        "@vector:[VECTOR_RANGE $range $query_vector]=>"
        "{$YIELD_DISTANCE_AS: vector_score}"
    )
    .sort_by("vector_score")
    .return_fields("vector_score", "id", "brand", "model", "description")
    .paging(0, 4)
    .dialect(2)
)
table = create_query_table(
    range_query, queries[:1],
    encoded_queries[:1],
    {"range": 0.55}
)
print(table)
# >>> | Bike for small kids |    0.52 | bikes:001 | Velorim    |...
# STEP_END
