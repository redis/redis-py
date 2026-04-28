"""
Redis Hash Operations
======================

Demonstrates working with Redis hashes -- lightweight dictionaries stored
under a single key.  Common use-cases include user profiles, product
catalogs, and session data.

Commands covered:
  - HSET / HGET / HGETALL
  - HMGET for fetching multiple fields
  - HINCRBY for atomic field increments
  - HDEL / HEXISTS / HKEYS / HVALS

Prerequisites:
  pip install redis

  A running Redis server (default: localhost:6379).
  Start one with Docker:
    docker run -d -p 6379:6379 redis
"""

import redis


def main() -> None:
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # ------------------------------------------------------------------
    # 1. HSET -- create a hash with multiple fields in one call
    # ------------------------------------------------------------------
    r.hset(
        "user:1000",
        mapping={
            "name": "Alice",
            "email": "alice@example.com",
            "login_count": 0,
            "verified": "true",
        },
    )
    print("Created user:1000")

    # ------------------------------------------------------------------
    # 2. HGET -- read a single field
    # ------------------------------------------------------------------
    name = r.hget("user:1000", "name")
    print(f"HGET name    -> {name}")  # Alice

    # ------------------------------------------------------------------
    # 3. HMGET -- read several fields at once
    # ------------------------------------------------------------------
    fields = r.hmget("user:1000", ["name", "email"])
    print(f"HMGET        -> {fields}")  # ['Alice', 'alice@example.com']

    # ------------------------------------------------------------------
    # 4. HGETALL -- return every field-value pair as a dict
    # ------------------------------------------------------------------
    user = r.hgetall("user:1000")
    print(f"HGETALL      -> {user}")

    # ------------------------------------------------------------------
    # 5. HINCRBY -- atomically increment a numeric field
    # ------------------------------------------------------------------
    r.hincrby("user:1000", "login_count", 1)
    r.hincrby("user:1000", "login_count", 1)
    logins = r.hget("user:1000", "login_count")
    print(f"login_count  -> {logins}")  # 2

    # ------------------------------------------------------------------
    # 6. HEXISTS / HKEYS / HVALS -- inspect hash structure
    # ------------------------------------------------------------------
    exists = r.hexists("user:1000", "email")
    print(f"HEXISTS email -> {exists}")  # True

    keys = r.hkeys("user:1000")
    print(f"HKEYS        -> {keys}")

    vals = r.hvals("user:1000")
    print(f"HVALS        -> {vals}")

    # ------------------------------------------------------------------
    # 7. HDEL -- remove specific fields from the hash
    # ------------------------------------------------------------------
    r.hdel("user:1000", "verified")
    remaining = r.hgetall("user:1000")
    print(f"After HDEL   -> {remaining}")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    r.delete("user:1000")
    print("Cleanup complete.")


if __name__ == "__main__":
    main()
