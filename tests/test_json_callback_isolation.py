"""
Regression tests for issue #3937:
  calling r.json() mutates the parent Redis client's response_callbacks,
  causing subsequent bare execute_command("JSON.*") calls to return decoded
  dicts instead of raw bytes even when decode_responses=False.

These tests are pure-unit (no live Redis required) and use
@pytest.mark.fixed_client so they run in every CI environment.
"""

import pytest
import redis


@pytest.mark.fixed_client
def test_json_method_does_not_mutate_parent_response_callbacks():
    """
    Calling r.json() must not add JSON.* keys to the parent client's
    response_callbacks dict.

    Before the fix, _JSONBase.__init__ called
      self.client.set_response_callback(key, value)
    for every JSON command, permanently polluting the shared callback table.
    After the fix those callbacks should remain scoped to the JSON sub-client.
    """
    r = redis.Redis()

    json_keys_before = [k for k in r.response_callbacks if "JSON" in str(k).upper()]
    assert json_keys_before == [], (
        "Fresh Redis client should have no JSON.* callbacks, "
        f"but found: {json_keys_before}"
    )

    # Instantiate the JSON sub-client
    _ = r.json()

    json_keys_after = [k for k in r.response_callbacks if "JSON" in str(k).upper()]
    assert json_keys_after == [], (
        "r.json() must not inject JSON.* callbacks into the parent client, "
        f"but found after call: {sorted(json_keys_after)}"
    )


@pytest.mark.fixed_client
def test_json_method_called_multiple_times_does_not_accumulate_callbacks():
    """
    Calling r.json() repeatedly must not accumulate JSON.* callbacks on
    the parent client.
    """
    r = redis.Redis()
    _ = r.json()
    _ = r.json()
    _ = r.json()

    json_keys = [k for k in r.response_callbacks if "JSON" in str(k).upper()]
    assert json_keys == [], (
        "Multiple r.json() calls must not accumulate JSON.* callbacks "
        f"on the parent client, but found: {sorted(json_keys)}"
    )


@pytest.mark.fixed_client
def test_two_independent_clients_do_not_share_json_callbacks():
    """
    Two separate Redis client instances must have completely independent
    callback tables — adding JSON callbacks to one must not affect the other.
    """
    r1 = redis.Redis()
    r2 = redis.Redis()

    _ = r1.json()

    json_keys_r2 = [k for k in r2.response_callbacks if "JSON" in str(k).upper()]
    assert json_keys_r2 == [], (
        "Calling r1.json() must not inject JSON.* callbacks into an unrelated "
        f"client r2, but found on r2: {sorted(json_keys_r2)}"
    )


@pytest.mark.fixed_client
def test_json_subclient_callbacks_are_registered_on_json_object_not_parent():
    """
    The JSON.* response callbacks should be registered on the JSON sub-client
    object (accessible via its _MODULE_CALLBACKS attribute), NOT on the parent
    Redis client's response_callbacks.
    """
    r = redis.Redis()
    j = r.json()

    # The JSON sub-client must hold its own callbacks
    assert hasattr(j, "_MODULE_CALLBACKS"), (
        "JSON sub-client must expose _MODULE_CALLBACKS"
    )
    assert len(j._MODULE_CALLBACKS) > 0, (
        "JSON sub-client _MODULE_CALLBACKS must not be empty"
    )
    assert "JSON.GET" in j._MODULE_CALLBACKS, (
        "JSON.GET callback must exist on the JSON sub-client"
    )

    # But the parent client must remain unpolluted
    assert "JSON.GET" not in r.response_callbacks, (
        "JSON.GET must not be present on the parent Redis client's "
        "response_callbacks after r.json() is called"
    )


@pytest.mark.fixed_client
def test_execute_command_strips_redis_internal_kwargs():
    """
    JSON.execute_command must not forward redis-internal kwargs (e.g. ``keys``,
    ``options``) to the module callback.  Callbacks like _decode, bulk_of_jsons,
    and simple lambdas don't accept **kwargs, so passing those through causes a
    TypeError — which in some call paths (e.g. JSON.get) is silently swallowed
    and returns None instead of the actual data.

    The fix: only ``_json_path`` (and any other JSON-module kwargs) are
    forwarded; everything else is stripped.
    """
    sentinel = object()
    received_kwargs = {}

    def spy_callback(response, **kwargs):
        received_kwargs.update(kwargs)
        return sentinel

    r = redis.Redis()
    j = r.json()

    # Patch the callback for JSON.GET on this instance
    j._MODULE_CALLBACKS["JSON.GET"] = spy_callback

    # Monkeypatch the parent client so no real network call is made
    j.client.execute_command = lambda *a, **kw: b"raw"

    result = j.execute_command("JSON.GET", "mykey", keys=["mykey"], options={})

    assert result is sentinel, "Callback return value must be passed through"
    # redis-internal kwargs must have been stripped
    assert "keys" not in received_kwargs, (
        "redis-internal kwarg 'keys' must not be forwarded to the callback"
    )
    assert "options" not in received_kwargs, (
        "redis-internal kwarg 'options' must not be forwarded to the callback"
    )


@pytest.mark.fixed_client
def test_pipeline_merges_json_callbacks_into_cluster_pipeline():
    """
    j.pipeline() on a ClusterPipeline path must merge _MODULE_CALLBACKS into
    the pipeline's cluster_response_callbacks copy so JSON.* commands are
    decoded correctly.  Before the fix, the ClusterPipeline was constructed
    with the unmodified parent cluster_response_callbacks (which no longer
    contain JSON entries after we stopped registering them on the parent).
    """
    import unittest.mock as mock

    r = redis.Redis()
    j = r.json()

    # Simulate a minimal RedisCluster-like client
    fake_cluster = mock.MagicMock(spec=redis.RedisCluster)
    fake_cluster.cluster_response_callbacks = {"PING": lambda r: r}
    fake_cluster.response_callbacks = {}
    j.client = fake_cluster

    captured = {}

    def fake_cluster_pipeline(**kwargs):
        captured.update(kwargs)
        p = mock.MagicMock()
        p._encode = None
        p._decode = None
        return p

    with mock.patch(
        "redis.commands.json.ClusterPipeline", side_effect=fake_cluster_pipeline
    ):
        j.pipeline()

    crc = captured.get("cluster_response_callbacks", {})
    assert "JSON.GET" in crc, (
        "ClusterPipeline must receive JSON.GET in cluster_response_callbacks"
    )
    # The original parent callback must still be present (not replaced)
    assert "PING" in crc, (
        "Original cluster_response_callbacks entries must be preserved"
    )
