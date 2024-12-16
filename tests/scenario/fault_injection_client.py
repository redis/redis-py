import requests


class TriggeredAction:

    def __init__(self, client: "FaultInjectionClient", data: dict):
        self.client = client
        self.action_id = data["action_id"]
        self.data = data

    def refresh(self):
        self.data = self.client.get_action(self.action_id)

    @property
    def status(self):
        if "status" not in self.data:
            return "pending"
        return self.data["status"]

    def wait_until_complete(self):
        while self.status not in ("success", "failed"):
            self.refresh()
        return self.status


class FaultInjectionClient:
    def __init__(self, base_url: str = "http://127.0.0.1:20324"):
        self.base_url = base_url

    def trigger_action(self, action_type: str, parameters: dict):
        response = requests.post(
            f"{self.base_url}/action",
            json={"type": action_type, "parameters": parameters},
        )
        return TriggeredAction(self, response.json())

    def get_action(self, action_id: str):
        response = requests.get(f"{self.base_url}/action/{action_id}")
        return response.json()
