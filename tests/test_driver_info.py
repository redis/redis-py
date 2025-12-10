import pytest

from redis.driver_info import DriverInfo


def test_driver_info_default_name_no_upstream():
    info = DriverInfo()
    assert info.formatted_name == "redis-py"
    assert info.upstream_drivers == []


def test_driver_info_single_upstream():
    info = DriverInfo().add_upstream_driver("django-redis", "5.4.0")
    assert info.formatted_name == "redis-py(django-redis_v5.4.0)"


def test_driver_info_multiple_upstreams_latest_first():
    info = DriverInfo()
    info.add_upstream_driver("django-redis", "5.4.0")
    info.add_upstream_driver("celery", "5.4.1")
    assert info.formatted_name == "redis-py(celery_v5.4.1;django-redis_v5.4.0)"


@pytest.mark.parametrize(
    "name",
    [
        "DjangoRedis",  # must start with lowercase
        "django redis",  # spaces not allowed
        "django{redis}",  # braces not allowed
        "django:redis",  # ':' not allowed by validation regex
    ],
)
def test_driver_info_invalid_name(name):
    info = DriverInfo()
    with pytest.raises(ValueError):
        info.add_upstream_driver(name, "3.2.0")


@pytest.mark.parametrize(
    "version",
    [
        "3.2.0 beta",  # space not allowed
        "3.2.0)",  # brace not allowed
        "3.2.0\n",  # newline not allowed
    ],
)
def test_driver_info_invalid_version(version):
    info = DriverInfo()
    with pytest.raises(ValueError):
        info.add_upstream_driver("django-redis", version)
