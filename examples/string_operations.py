"""
Basic Redis String Operations
==============================

Demonstrates the most common Redis string commands using redis-py:
  - SET / GET for storing and retrieving values
  - MSET / MGET for bulk operations
  - INCR / DECR for atomic counters
  - Key expiration with EX and PX options

Prerequisites:
  pip install redis

  A running Redis server (default: localhost:6379).
  Start one with Docker:
    docker run -d -p 6379:6379 redis
"""

import redis


def main() -> None:
    # Connect to Redis. decode_responses=True ensures we get str instead
    # of bytes for every response.
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # ------------------------------------------------------------------
    # 1. SET and GET -- store and retrieve a single value
    # ------------------------------------------------------------------
    r.set("greeting", "Hello, Redis!")
    value = r.get("greeting")
    print(f"GET greeting -> {value}")  # Hello, Redis!

    # ------------------------------------------------------------------
    # 2. SET with expiration -- key auto-deletes after the TTL
    # ------------------------------------------------------------------
    # EX sets the expiry in seconds; PX would set it in milliseconds.
    r.set("temp_key", "I will expire", ex=60)
    ttl = r.ttl("temp_key")
    print(f"TTL temp_key -> {ttl}s")  # ~60

    # ------------------------------------------------------------------
    # 3. SET with NX / XX flags
    # ------------------------------------------------------------------
    # NX: only set if the key does NOT exist (useful for locking)
    r.set("bike:1", "Deimos")
    result_nx = r.set("bike:1", "Ares", nx=True)
    print(f"SET bike:1 NX -> {result_nx}")  # None (key already exists)

    # XX: only set if the key DOES exist
    result_xx = r.set("bike:1", "Ares", xx=True)
    print(f"SET bike:1 XX -> {result_xx}")  # True

    # ------------------------------------------------------------------
    # 4. MSET and MGET -- set/get multiple keys in one round-trip
    # ------------------------------------------------------------------
    r.mset({"color:1": "red", "color:2": "green", "color:3": "blue"})
    colors = r.mget(["color:1", "color:2", "color:3"])
    print(f"MGET colors  -> {colors}")  # ['red', 'green', 'blue']

    # ------------------------------------------------------------------
    # 5. Atomic counters -- INCR / INCRBY / DECR / DECRBY
    # ------------------------------------------------------------------
    r.set("page_views", 0)
    r.incr("page_views")          # 1
    r.incrby("page_views", 10)    # 11
    r.decr("page_views")          # 10
    views = r.get("page_views")
    print(f"page_views   -> {views}")  # 10

    # ------------------------------------------------------------------
    # Cleanup -- remove keys created by this example
    # ------------------------------------------------------------------
    r.delete(
        "greeting", "temp_key", "bike:1",
        "color:1", "color:2", "color:3",
        "page_views",
    )
    print("Cleanup complete.")


if __name__ == "__main__":
    main()
