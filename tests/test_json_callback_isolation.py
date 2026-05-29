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
