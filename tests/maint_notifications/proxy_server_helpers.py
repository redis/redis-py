import base64
from dataclasses import dataclass
import logging
import re
from typing import Union

from redis.http.http_client import HttpClient, HttpError


class RespTranslator:
    """Helper class to translate between RESP and other encodings."""

    @staticmethod
    def str_or_list_to_resp(txt: str) -> str:
        """
        Convert specific string or list to RESP format.
        """
        if re.match(r"^<.*>$", txt):
            items = txt[1:-1].split(",")
            return f"*{len(items)}\r\n" + "\r\n".join(
                f"${len(x)}\r\n{x}" for x in items
            )
        else:
            return f"${len(txt)}\r\n{txt}"

    @staticmethod
    def cluster_slots_to_resp(resp: str) -> str:
        """Convert query to RESP format."""
        return (
            f"*{len(resp.split())}\r\n"
            + "\r\n".join(f"${len(x)}\r\n{x}" for x in resp.split())
            + "\r\n"
        )

    @staticmethod
    def oss_maint_notification_to_resp(resp: str) -> str:
        """Convert query to RESP format."""
        return (
            f">{len(resp.split())}\r\n"
            + "\r\n".join(
                f"{RespTranslator.str_or_list_to_resp(x)}" for x in resp.split()
            )
            + "\r\n"
        )


@dataclass
class SlotsRange:
    host: str
    port: int
    start_slot: int
    end_slot: int


class ProxyInterceptorHelper:
    """Helper class for intercepting socket calls and managing interceptor server."""

    def __init__(self, server_url: str = "http://localhost:4000"):
        self.server_url = server_url
        self._resp_translator = RespTranslator()
        self.http_client = HttpClient()
        self._interceptors = list()

    def cleanup_interceptors(self, *names: str):
        """
        Resets all the interceptors by providing empty pattern and returned response.

        Args:
            names: Names of the interceptors to reset
        """
        if not names:
            names = self._interceptors
        for name in tuple(names):
            self._reset_interceptor(name)

    def set_cluster_slots(
        self,
        name: str,
        slots_ranges: list[SlotsRange],
    ) -> str:
        """
        Set cluster slots and nodes by intercepting CLUSTER SLOTS command.

        This method creates an interceptor that intercepts CLUSTER SLOTS commands
        and returns a modified topology with the provided data.

        Args:
            name: Name of the interceptor
            slots_ranges: List of SlotsRange objects representing the cluster
                nodes and slots coverage

        Returns:
            The interceptor name that was created

        Example:
            interceptor = ProxyInterceptorHelper(None, "http://localhost:4000")
            interceptor.set_cluster_slots(
                "test_topology",
                [
                    SlotsRange("127.0.0.1", 6379, 0, 5000),
                    SlotsRange("127.0.0.1", 6380, 5001, 10000),
                    SlotsRange("127.0.0.1", 6381, 10001, 16383),
                ]
            )
        """
        # Build RESP response for CLUSTER SLOTS
        # Format: *<num_slots_ranges> for each range: *3 :start :end *3 $<host_len> <host> :<port> $<id_len> <id>
        resp_parts = [f"*{len(slots_ranges)}"]

        for slots_range in slots_ranges:
            # Node info: *3 for (host, port, id)
            resp_parts.append("*3")
            # 1st elem --> start slot
            resp_parts.append(f":{slots_range.start_slot}")
            # 2nd elem --> end slot
            resp_parts.append(f":{slots_range.end_slot}")

            # 3rd elem --> list with node details: *4 for (host, port, id, empty hash)
            resp_parts.append("*4")
            # 1st elem --> host
            resp_parts.append(f"${len(slots_range.host)}")
            resp_parts.append(f"{slots_range.host}")
            # 2nd elem --> port
            resp_parts.append(f":{slots_range.port}")
            # 3rd elem --> node id
            node_id = f"proxy-id-{slots_range.port}"
            resp_parts.append(f"${len(node_id)}")
            resp_parts.append(node_id)
            # 4th elem --> empty hash
            resp_parts.append("$0")
            resp_parts.append("")

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
            if isinstance(response, dict):
                return response
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
            if isinstance(response, dict):
                return response
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
            response = self.http_client.post(url, data=data)
            if isinstance(response, dict):
                return response
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
            self._interceptors.append(name)
            if isinstance(proxy_response, dict):
                return proxy_response
            return proxy_response.json() if proxy_response else {}
        except HttpError as e:
            raise RuntimeError(f"Failed to add interceptor: {e}")

    def _reset_interceptor(self, name: str):
        """
        Reset an interceptor by providing empty pattern and returned response.

        Args:
            name: Name of the interceptor to reset
        """
        self._add_interceptor(name, "no_match", "")
