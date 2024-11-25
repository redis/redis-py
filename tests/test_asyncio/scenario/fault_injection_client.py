import aiohttp


class TriggeredAction:

    def __init__(self, client: "AsyncFaultInjectionClient", data: dict):
        self.client = client
        self.action_id = data["action_id"]
        self.data = data

    async def refresh(self):
        self.data = await self.client.get_action(self.action_id)

    @property
    def status(self):
        if "status" not in self.data:
            return "pending"
        return self.data["status"]

    async def wait_until_complete(self):
        while self.status not in ("success", "failed"):
            await self.refresh()
        return self.status


class AsyncFaultInjectionClient:
    def __init__(self, base_url: str = "http://127.0.0.1:20324"):
        self.base_url = base_url
        self.session = aiohttp.ClientSession()

    async def trigger_action(self, action_type: str, parameters: dict):
        async with self.session.post(
            f"{self.base_url}/action",
            json={"type": action_type, "parameters": parameters},
        ) as response:
            return TriggeredAction(self, await response.json())

    async def get_action(self, action_id: str):
        async with self.session.get(f"{self.base_url}/action/{action_id}") as response:
            return await response.json()
