import dataclasses
import json
import os.path
from urllib.parse import urlparse

import pytest


@dataclasses.dataclass
class Endpoint:
    bdb_id: int
    username: str
    password: str
    tls: bool
    endpoints: list[str]

    @property
    def url(self):
        parsed_url = urlparse(self.endpoints[0])

        if self.tls:
            parsed_url = parsed_url._replace(scheme="rediss")

        domain = parsed_url.netloc.split("@")[-1]
        domain = f"{self.username}:{self.password}@{domain}"

        parsed_url = parsed_url._replace(netloc=domain)

        return parsed_url.geturl()

    @classmethod
    def from_dict(cls, data: dict):
        field_names = set(f.name for f in dataclasses.fields(cls))
        return cls(**{k: v for k, v in data.items() if k in field_names})


def get_endpoint(request: pytest.FixtureRequest, endpoint_name: str) -> Endpoint:
    endpoints_config_path = request.config.getoption("--endpoints-config")

    if not (endpoints_config_path and os.path.exists(endpoints_config_path)):
        raise FileNotFoundError(
            f"Endpoints config file not found: {endpoints_config_path}"
        )

    try:
        with open(endpoints_config_path, "r") as f:
            endpoints_config = json.load(f)
    except Exception as e:
        raise ValueError(
            f"Failed to load endpoints config file: {endpoints_config_path}"
        ) from e

    if not (isinstance(endpoints_config, dict) and endpoint_name in endpoints_config):
        raise ValueError(f"Endpoint not found in config: {endpoint_name}")

    return Endpoint.from_dict(endpoints_config.get(endpoint_name))
