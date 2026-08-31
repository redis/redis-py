"""Unit tests for search aggregation request building.

These tests exercise the pure-Python argument building logic and do not
require a running Redis server, so they live in the ``fixed_client`` test
group.
"""

import pytest

from redis.commands.search import reducers
from redis.commands.search.aggregation import FIELDNAME, AggregateRequest


@pytest.mark.fixed_client
class TestReducerAlias:
    def test_fieldname_alias_with_at_prefix(self):
        reducer = reducers.sum("@paid").alias(FIELDNAME)
        assert reducer._alias == "paid"

    def test_fieldname_alias_without_at_prefix(self):
        # The '@' prefix is optional, so the name must be used as-is rather
        # than having its first character removed.
        reducer = reducers.sum("paid").alias(FIELDNAME)
        assert reducer._alias == "paid"

    def test_fieldname_alias_without_at_prefix_in_args(self):
        request = AggregateRequest("*").group_by(
            "@id", reducers.sum("paid").alias(FIELDNAME)
        )
        assert request.build_args()[-3:] == ["paid", "AS", "paid"]

    def test_fieldname_alias_without_field(self):
        with pytest.raises(ValueError, match="Cannot use FIELDNAME alias"):
            reducers.count().alias(FIELDNAME)
