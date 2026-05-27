import pytest

from redis._parsers.socket import SENTINEL as PARSER_SENTINEL
from redis.driver_info import DriverInfo, resolve_driver_info
from redis.utils import SENTINEL, get_lib_version


def test_driver_info_default_name_no_upstream():
    info = DriverInfo()
    assert info.formatted_name == "redis-py"
    assert info.upstream_drivers == []
    assert info.lib_version == get_lib_version()


def test_driver_info_custom_lib_version():
    info = DriverInfo(lib_version="5.0.0")
    assert info.lib_version == "5.0.0"
    assert info.formatted_name == "redis-py"


def test_driver_info_explicit_none_values_are_preserved():
    info = DriverInfo(name=None, lib_version=None)
    assert info.formatted_name is None
    assert info.lib_version is None


def test_resolve_driver_info_default_values():
    info = resolve_driver_info()
    assert info.formatted_name == "redis-py"
    assert info.lib_version == get_lib_version()


def test_parser_sentinel_uses_shared_sentinel():
    assert PARSER_SENTINEL is SENTINEL


@pytest.mark.parametrize(
    ("driver_info", "lib_name", "lib_version"),
    [
        (None, SENTINEL, SENTINEL),
        (SENTINEL, None, None),
    ],
)
def test_resolve_driver_info_explicit_none_skips_config(
    driver_info, lib_name, lib_version
):
    assert resolve_driver_info(driver_info, lib_name, lib_version) is None


def test_resolve_driver_info_explicit_none_values_are_individual():
    info = resolve_driver_info(lib_name=None)
    assert info.formatted_name is None
    assert info.lib_version == get_lib_version()

    info = resolve_driver_info(lib_version=None)
    assert info.formatted_name == "redis-py"
    assert info.lib_version is None


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
