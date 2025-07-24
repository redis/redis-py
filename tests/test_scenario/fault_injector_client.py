import json
import urllib.request
from typing import Dict, Any, Optional, Union
from enum import Enum


class ActionType(str, Enum):
    DMC_RESTART = "dmc_restart"
    FAILOVER = "failover"
    RESHARD = "reshard"
    SEQUENCE_OF_ACTIONS = "sequence_of_actions"
    NETWORK_FAILURE = "network_failure"
    EXECUTE_RLUTIL_COMMAND = "execute_rlutil_command"


class RestartDmcParams:
    def __init__(self, bdb_id: str):
        self.bdb_id = bdb_id

    def to_dict(self) -> Dict[str, str]:
        return {"bdb_id": self.bdb_id}


class ActionRequest:
    def __init__(self, action_type: ActionType, parameters: Union[Dict[str, Any], RestartDmcParams]):
        self.type = action_type
        self.parameters = parameters

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "parameters": self.parameters.to_dict() if isinstance(self.parameters,
                                                                  RestartDmcParams) else self.parameters
        }


class FaultInjectorClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')

    def _make_request(self, method: str, path: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json"} if data else {}

        request = urllib.request.Request(
            url,
            method=method,
            data=json.dumps(data).encode('utf-8') if data else None,
            headers=headers
        )

        try:
            with urllib.request.urlopen(request) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code == 422:
                error_body = json.loads(e.read().decode('utf-8'))
                raise ValueError(f"Validation Error: {error_body}")
            raise

    def list_actions(self) -> Dict[str, Any]:
        """List all available actions"""
        return self._make_request("GET", "/action")

    def trigger_action(self, action_request: ActionRequest) -> Dict[str, Any]:
        """Trigger a new action"""
        return self._make_request("POST", "/action", action_request.to_dict())

    def get_action_status(self, action_id: str) -> Dict[str, Any]:
        """Get the status of a specific action"""
        return self._make_request("GET", f"/action/{action_id}")
