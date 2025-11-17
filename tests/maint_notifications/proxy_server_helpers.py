import base64
import logging
from typing import Union

from redis.http.http_client import HttpClient, HttpError
# from urllib.request import Request, urlopen
# from urllib.error import URLError


class RespTranslator:
    """Helper class to translate between RESP and other encodings."""

    @staticmethod
    def cluster_slots_to_resp(resp: str) -> str:
        """Convert query to RESP format."""
        return (
            f"*{len(resp.split())}\r\n"
            + "\r\n".join(f"${len(x)}\r\n{x}" for x in resp.split())
            + "\r\n"
        )

    @staticmethod
    def smigrating_to_resp(resp: str) -> str:
        """Convert query to RESP format."""
        return (
            f">{len(resp.split())}\r\n"
            + "\r\n".join(f"${len(x)}\r\n{x}" for x in resp.split())
            + "\r\n"
        )


class ProxyInterceptorHelper:
    """Helper class for intercepting socket calls and managing interceptor server."""

    def __init__(self, server_url: str = "http://localhost:4000"):
        self.server_url = server_url
        self._resp_translator = RespTranslator()
        self.http_client = HttpClient()

    def cleanup_interceptors(self, *names: str):
        """
        Resets all the interceptors by providing empty pattern and returned response.

        Args:
            names: Names of the interceptors to reset
        """
        for name in names:
            self._reset_interceptor(name)

    def set_cluster_nodes(self, name: str, nodes: list[tuple[str, int]]) -> str:
        """
        Set cluster nodes by intercepting CLUSTER SLOTS command.

        This method creates an interceptor that intercepts CLUSTER SLOTS commands
        and returns a modified topology with the provided nodes.

        Args:
            name: Name of the interceptor
            nodes: List of (host, port) tuples representing the cluster nodes

        Returns:
            The interceptor name that was created

        Example:
            interceptor = ProxyInterceptorHelper(None, "http://localhost:4000")
            interceptor_name = interceptor.set_cluster_nodes(
                "test_topology",
                [("127.0.0.1", 6379), ("127.0.0.1", 6380), ("127.0.0.1", 6381)]
            )
        """
        # Build RESP response for CLUSTER SLOTS
        # Format: *<num_slots_ranges> for each range: *3 :start :end *3 $<host_len> <host> :<port> $<id_len> <id>
        resp_parts = [f"*{len(nodes)}"]

        # For simplicity, distribute slots evenly across nodes
        total_slots = 16384
        slots_per_node = total_slots // len(nodes)

        for i, (host, port) in enumerate(nodes):
            start_slot = i * slots_per_node
            end_slot = (
                (i + 1) * slots_per_node - 1 if i < len(nodes) - 1 else total_slots - 1
            )

            # Node info: *3 for (host, port, id)
            resp_parts.append("*3")
            resp_parts.append(f":{start_slot}")
            resp_parts.append(f":{end_slot}")

            # Node details: *3 for (host, port, id)
            resp_parts.append("*3")
            resp_parts.append(f"${len(host)}")
            resp_parts.append(host)
            resp_parts.append(f":{port}")
            resp_parts.append("$13")
            resp_parts.append(f"proxy-id-{port}")

        response = "\r\n".join(resp_parts) + "\r\n"

        # Add the interceptor
        self._add_interceptor(
            name=name,
            match="*2\r\n$7\r\ncluster\r\n$5\r\nslots\r\n",
            response=response,
            encoding="raw",
        )

        return name

    def get_stats(self) -> dict:
        """
        Get statistics from the interceptor server.

        Returns:
            Statistics dictionary containing connection information
        """
        url = f"{self.server_url}/stats"

        try:
            response = self.http_client.get(url)
            return response.json()

        except HttpError as e:
            raise RuntimeError(f"Failed to get stats from interceptor server: {e}")

    def get_connections(self) -> dict:
        """
        Get all active connections from the server.

        Returns:
            Response from the server as a dictionary
        """
        url = f"{self.server_url}/connections"

        try:
            response = self.http_client.get(url)
            return response.json()
        except HttpError as e:
            raise RuntimeError(f"Failed to get connections: {e}")

    def send_notification(
        self,
        connected_to_port: Union[int, str],
        notification: str,
    ) -> dict:
        """
        Send a notification to all connections connected to
        a specific node(identified by port number).

        This method:
        1. Fetches stats from the interceptor server
        2. Finds all connection IDs connected to the specified node
        3. Sends the notification to each connection

        Args:
            connected_to_port: Port number of the node to send the notification to
            notification: The notification message to send (in RESP format)

        Returns:
            Response from the server as a dictionary

        Example:
            interceptor = ProxyInterceptorHelper(None, "http://localhost:4000")
            result = interceptor.send_notification(
                "6379",
                "KjENCiQ0DQpQSU5HDQo="  # PING command in base64
            )
        """
        # Get stats to find connection IDs for the node
        stats = self.get_stats()

        # Extract connection IDs for the specified node
        conn_ids = []
        for node_key, node_info in stats.items():
            node_port = node_key.split("@")[1]
            if int(node_port) == int(connected_to_port):
                for conn in node_info.get("connections", []):
                    conn_ids.append(conn["id"])

        if not conn_ids:
            raise RuntimeError(
                f"No connections found for node {node_port}. "
                f"Available nodes: {list(set(c.get('node') for c in stats.get('connections', {}).values()))}"
            )

        # Send notification to each connection
        results = {}
        logging.info(f"Sending notification to {len(conn_ids)} connections: {conn_ids}")
        connections_query = f"connectionIds={','.join(conn_ids)}"
        url = f"{self.server_url}/send-to-clients?{connections_query}&encoding=base64"
        # Encode notification to base64
        data = base64.b64encode(notification.encode("utf-8"))

        try:
            response = self.http_client.post(url, json_body=data)
            results = response.json()
        except HttpError as e:
            results = {"error": str(e)}

        return {
            "node_address": node_port,
            "connection_ids": conn_ids,
            "results": results,
        }

    def _add_interceptor(
        self,
        name: str,
        match: str,
        response: str,
        encoding: str = "raw",
    ) -> dict:
        """
        Add an interceptor to the server.

        Args:
            name: Name of the interceptor
            match: Pattern to match (RESP format)
            response: Response to return when matched (RESP format)
            encoding: Encoding type - "base64" or "raw"

        Returns:
            Response from the server as a dictionary
        """
        url = f"{self.server_url}/interceptors"
        payload = {
            "name": name,
            "match": match,
            "response": response,
            "encoding": encoding,
        }
        headers = {"Content-Type": "application/json"}

        try:
            proxy_response = self.http_client.post(
                url, json_body=payload, headers=headers
            )
            return proxy_response.json()
        except HttpError as e:
            raise RuntimeError(f"Failed to add interceptor: {e}")

    def _reset_interceptor(self, name: str):
        """
        Reset an interceptor by providing empty pattern and returned response.

        Args:
            name: Name of the interceptor to reset
        """
        self._add_interceptor(name, "", "")
