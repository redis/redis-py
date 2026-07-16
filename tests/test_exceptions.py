from redis.exceptions import (
    CrossSlotTransactionError,
    InvalidPipelineStack,
    LockError,
    LockNotOwnedError,
    RedisClusterException,
    SlotNotCoveredError,
)


class TestLockError:
    def test_defaults(self):
        err = LockError()
        assert err.message is None
        assert err.lock_name is None
        assert err.args == (None,)

    def test_with_message_and_lock_name(self):
        err = LockError("lock is not owned", "my-lock")
        assert err.message == "lock is not owned"
        assert err.lock_name == "my-lock"
        assert err.args == ("lock is not owned",)

    def test_is_value_error(self):
        assert isinstance(LockError(), ValueError)

    def test_lock_not_owned_error_inherits_lock_error(self):
        err = LockNotOwnedError("not owned", "my-lock")
        assert isinstance(err, LockError)
        assert err.message == "not owned"
        assert err.lock_name == "my-lock"


class TestRedisClusterException:
    def test_sets_error_type_and_args(self):
        err = RedisClusterException("boom")
        assert err.args == ("boom",)
        assert repr(err) == "server:RedisClusterException"

    def test_subclasses_construct(self):
        assert SlotNotCoveredError("no node covers slot").args == (
            "no node covers slot",
        )
        assert CrossSlotTransactionError("cross slot").args == ("cross slot",)
        assert InvalidPipelineStack("bad stack").args == ("bad stack",)
