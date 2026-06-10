"""
Endpoint resolution helpers for the multi-database (active-active) integration
tests.

Defaults map directly onto the ``docker-compose.yml`` infrastructure:

* ``cluster``    -> the ``cluster`` (``16379``) and ``cluster2`` (``16385``)
  services, i.e. two independent Redis clusters;
* ``standalone`` -> the ``redis`` (``6379``) and ``redis-stack`` (``6479``)
  services, i.e. two independent standalone servers.

Both can be overridden via a comma-separated env var
``REDIS_MULTIDB_<TOPOLOGY>_URLS`` (e.g. ``REDIS_MULTIDB_CLUSTER_URLS``).
"""

import os

DEFAULT_ENDPOINTS = {
    "cluster": ("redis://localhost:16379", "redis://localhost:16385"),
    "standalone": ("redis://localhost:6379", "redis://localhost:6479"),
}


def get_endpoints(topology: str) -> tuple:
    """Return the two database URLs for the given topology."""
    env = os.getenv(f"REDIS_MULTIDB_{topology.upper()}_URLS")
    if env:
        urls = tuple(u.strip() for u in env.split(",") if u.strip())
    else:
        urls = DEFAULT_ENDPOINTS[topology]

    if len(urls) < 2:
        raise ValueError(
            f"MultiDBClient integration tests require at least two endpoints "
            f"for topology '{topology}', got: {urls}"
        )
    return urls
