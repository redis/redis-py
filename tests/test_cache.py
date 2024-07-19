import time

from redis import Redis, RedisCluster


def test_standalone_cached_get_and_set():
    r = Redis(use_cache=True, protocol=3)
    assert r.set("key", 5)
    assert r.get("key") == b"5"

    r2 = Redis(protocol=3)
    r2.set("key", "foo")

    time.sleep(0.5)

    after_invalidation = r.get("key")
    print(f'after invalidation {after_invalidation}')
    assert after_invalidation == b"foo"


def test_cluster_cached_get_and_set():
    cluster_url = "redis://localhost:16379/0"

    r = RedisCluster.from_url(cluster_url, use_cache=True, protocol=3)
    assert r.set("key", 5)
    assert r.get("key") == b"5"

    r2 = RedisCluster.from_url(cluster_url, use_cache=True, protocol=3)
    r2.set("key", "foo")

    time.sleep(0.5)
    
    after_invalidation = r.get("key")
    print(f'after invalidation {after_invalidation}')
    assert after_invalidation == b"foo"
