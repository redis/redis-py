from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

_BRACES = {"(", ")", "[", "]", "{", "}"}


def _validate_no_invalid_chars(value: str, field_name: str) -> None:
    """Ensure value contains only printable ASCII without spaces or braces.

    This mirrors the constraints enforced by other Redis clients for values that
    will appear in CLIENT LIST / CLIENT INFO output.
    """

    for ch in value:
        # printable ASCII without space: '!' (0x21) to '~' (0x7E)
        if ord(ch) < 0x21 or ord(ch) > 0x7E or ch in _BRACES:
            raise ValueError(
                f"{field_name} must not contain spaces, newlines, non-printable characters, or braces"
            )


def _validate_driver_name(name: str) -> None:
    """Validate an upstream driver name.

    The name should look like a typical Python distribution or package name,
    following a simplified form of PEP 503 normalisation rules:

    * start with a lowercase ASCII letter
    * contain only lowercase letters, digits, hyphens and underscores

    Examples of valid names: ``"django-redis"``, ``"celery"``, ``"rq"``.
    """

    import re

    _validate_no_invalid_chars(name, "Driver name")
    if not re.match(r"^[a-z][a-z0-9_-]*$", name):
        raise ValueError(
            "Upstream driver name must use a Python package-style name: "
            "start with a lowercase letter and contain only lowercase letters, "
            "digits, hyphens, and underscores (e.g., 'django-redis')."
        )


def _validate_driver_version(version: str) -> None:
    _validate_no_invalid_chars(version, "Driver version")


def _format_driver_entry(driver_name: str, driver_version: str) -> str:
    return f"{driver_name}_v{driver_version}"


@dataclass
class DriverInfo:
    """Driver information used to build the CLIENT SETINFO LIB-NAME value.

    The formatted name follows the pattern::

        name(driver1_vVersion1;driver2_vVersion2)

    Examples
    --------
    >>> info = DriverInfo()
    >>> info.formatted_name
    'redis-py'

    >>> info = DriverInfo().add_upstream_driver("django-redis", "5.4.0")
    >>> info.formatted_name
    'redis-py(django-redis_v5.4.0)'
    """

    name: str = "redis-py"
    _upstream: List[str] = field(default_factory=list)

    @property
    def upstream_drivers(self) -> List[str]:
        """Return a copy of the upstream driver entries.

        Each entry is in the form ``"driver-name_vversion"``.
        """

        return list(self._upstream)

    def add_upstream_driver(
        self, driver_name: str, driver_version: str
    ) -> "DriverInfo":
        """Add an upstream driver to this instance and return self.

        The most recently added driver appears first in :pyattr:`formatted_name`.
        """

        if driver_name is None:
            raise ValueError("Driver name must not be None")
        if driver_version is None:
            raise ValueError("Driver version must not be None")

        _validate_driver_name(driver_name)
        _validate_driver_version(driver_version)

        entry = _format_driver_entry(driver_name, driver_version)
        # insert at the beginning so latest is first
        self._upstream.insert(0, entry)
        return self

    @property
    def formatted_name(self) -> str:
        """Return the base name with upstream drivers encoded, if any.

        With no upstream drivers, this is just :pyattr:`name`. Otherwise::

            name(driver1_vX;driver2_vY)
        """

        if not self._upstream:
            return self.name
        return f"{self.name}({';'.join(self._upstream)})"
