import pytest
from redis.utils import compare_versions


@pytest.mark.parametrize(
    "version1,version2,expected_res",
    [
        ("1.0.0", "0.9.0", -1),
        ("1.0.0", "1.0.0", 0),
        ("0.9.0", "1.0.0", 1),
        ("1.09.0", "1.9.0", 0),
        ("1.090.0", "1.9.0", -1),
        ("1", "0.9.0", -1),
        ("1", "1.0.0", 0),
    ],
    ids=[
        "version1 > version2",
        "version1 == version2",
        "version1 < version2",
        "version1 == version2 - different minor format",
        "version1 > version2 - different minor format",
        "version1 > version2 - major version only",
        "version1 == version2 - major version only",
    ],
)
def test_compare_versions(version1, version2, expected_res):
    assert compare_versions(version1, version2) == expected_res
