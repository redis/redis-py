"""Unit tests for the search query string builders.

These tests exercise the pure-Python query building logic and do not require a
running Redis server, so they live in the ``fixed_client`` test group.
"""

import pytest

from redis.commands.search.querystring import GeoValue, geo, intersect


@pytest.mark.fixed_client
class TestGeoValue:
    """``geo()`` must render longitude before latitude.

    RediSearch expects geo filters as ``[lon lat radius unit]``, which is also
    the order used by ``GeoValue`` and ``query.GeoFilter``.
    """

    def test_geo_renders_longitude_before_latitude(self):
        assert geo(lat=51.45, lon=-0.44, radius=10).to_string() == "[-0.44 51.45 10 km]"

    def test_geo_matches_geovalue_built_directly(self):
        assert (
            geo(lat=51.45, lon=-0.44, radius=10).to_string()
            == GeoValue(-0.44, 51.45, 10).to_string()
        )

    def test_geo_keeps_the_requested_unit(self):
        assert (
            geo(lat=51.45, lon=-0.44, radius=10, unit="mi").to_string()
            == "[-0.44 51.45 10 mi]"
        )

    def test_geo_in_a_query(self):
        query = intersect(location=geo(lat=51.45, lon=-0.44, radius=10))
        assert query.to_string() == "@location:[-0.44 51.45 10 km]"
