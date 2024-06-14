# EXAMPLE: topk_tutorial
# HIDE_START
"""
Code samples for Top-K pages:
    https://redis.io/docs/latest/develop/data-types/probabilistic/top-k/
"""

import redis

r = redis.Redis(decode_responses=True)
# HIDE_END

# REMOVE_START
r.delete("bikes:keywords")
# REMOVE_END

# STEP_START topk
res1 = r.topk().reserve("bikes:keywords", 5, 2000, 7, 0.925)
print(res1)  # >>> True

res2 = r.topk().add(
    "bikes:keywords",
    "store",
    "seat",
    "handlebars",
    "handles",
    "pedals",
    "tires",
    "store",
    "seat",
)
print(res2)  # >>> [None, None, None, None, None, 'handlebars', None, None]

res3 = r.topk().list("bikes:keywords")
print(res3)  # >>> ['store', 'seat', 'pedals', 'tires', 'handles']

res4 = r.topk().query("bikes:keywords", "store", "handlebars")
print(res4)  # >>> [1, 0]
