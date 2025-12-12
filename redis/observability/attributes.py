"""
OpenTelemetry semantic convention attributes for Redis.

This module provides constants and helper functions for building OTel attributes
according to the semantic conventions for database clients.

Reference: https://opentelemetry.io/docs/specs/semconv/database/redis/
"""
from enum import Enum
from typing import Any, Dict, Optional

# Database semantic convention attributes
DB_SYSTEM = "db.system"
DB_NAMESPACE = "db.namespace"
DB_OPERATION_NAME = "db.operation.name"
DB_OPERATION_BATCH_SIZE = "db.operation.batch.size"
DB_RESPONSE_STATUS_CODE = "db.response.status_code"
DB_STORED_PROCEDURE_NAME = "db.stored_procedure.name"

# Error attributes
ERROR_TYPE = "error.type"

# Network attributes
NETWORK_PEER_ADDRESS = "network.peer.address"
NETWORK_PEER_PORT = "network.peer.port"

# Server attributes
SERVER_ADDRESS = "server.address"
SERVER_PORT = "server.port"

# Connection pool attributes
DB_CLIENT_CONNECTION_POOL_NAME = "db.client.connection.pool.name"
DB_CLIENT_CONNECTION_STATE = "db.client.connection.state"

# Redis-specific attributes
REDIS_CLIENT_LIBRARY = "redis.client.library"
REDIS_CLIENT_CONNECTION_PUBSUB = "redis.client.connection.pubsub"
REDIS_CLIENT_CONNECTION_CLOSE_REASON = "redis.client.connection.close.reason"
REDIS_CLIENT_CONNECTION_NOTIFICATION = "redis.client.connection.notification"
REDIS_CLIENT_OPERATION_RETRY_ATTEMPTS = "redis.client.operation.retry_attempts"
REDIS_CLIENT_OPERATION_BLOCKING = "redis.client.operation.blocking"
REDIS_CLIENT_PUBSUB_MESSAGE_DIRECTION = "redis.client.pubsub.message.direction"
REDIS_CLIENT_PUBSUB_CHANNEL = "redis.client.pubsub.channel"
REDIS_CLIENT_PUBSUB_SHARDED = "redis.client.pubsub.sharded"
REDIS_CLIENT_ERROR_INTERNAL = "redis.client.errors.internal"
REDIS_CLIENT_STREAM_NAME = "redis.client.stream.name"
REDIS_CLIENT_CONSUMER_GROUP = "redis.client.consumer_group"
REDIS_CLIENT_CONSUMER_NAME = "redis.client.consumer_name"

class ConnectionState(Enum):
    IDLE = "idle"
    USED = "used"

class PubSubDirection(Enum):
    PUBLISH = "publish"
    RECEIVE = "receive"


class AttributeBuilder:
    """
    Helper class to build OTel semantic convention attributes for Redis operations.
    """

    @staticmethod
    def build_base_attributes(
            server_address: Optional[str] = None,
            server_port: Optional[int] = None,
            db_namespace: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Build base attributes common to all Redis operations.

        Args:
            server_address: Redis server address (FQDN or IP)
            server_port: Redis server port
            db_namespace: Redis database index

        Returns:
            Dictionary of base attributes
        """
        attrs: Dict[str, Any] = {
            DB_SYSTEM: "redis",
            REDIS_CLIENT_LIBRARY: "redis-py"
        }

        if server_address is not None:
            attrs[SERVER_ADDRESS] = server_address

        if server_port is not None:
            attrs[SERVER_PORT] = server_port

        if db_namespace is not None:
            attrs[DB_NAMESPACE] = str(db_namespace)

        return attrs

    @staticmethod
    def build_operation_attributes(
            command_name: Optional[str] = None,
            batch_size: Optional[int] = None,
            network_peer_address: Optional[str] = None,
            network_peer_port: Optional[int] = None,
            stored_procedure_name: Optional[str] = None,
            retry_attempts: Optional[int] = None,
            is_blocking: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Build attributes for a Redis operation (command execution).

        Args:
            command_name: Redis command name (e.g., 'GET', 'SET', 'MULTI')
            batch_size: Number of commands in batch (for pipelines/transactions)
            network_peer_address: Resolved peer address
            network_peer_port: Peer port number
            stored_procedure_name: Lua script name or SHA1 digest
            retry_attempts: Number of retry attempts made
            is_blocking: Whether the operation is a blocking command

        Returns:
            Dictionary of operation attributes
        """
        attrs: Dict[str, Any] = {}

        if command_name is not None:
            attrs[DB_OPERATION_NAME] = command_name.upper()

        if batch_size is not None and batch_size >= 2:
            attrs[DB_OPERATION_BATCH_SIZE] = batch_size

        if network_peer_address is not None:
            attrs[NETWORK_PEER_ADDRESS] = network_peer_address

        if network_peer_port is not None:
            attrs[NETWORK_PEER_PORT] = network_peer_port

        if stored_procedure_name is not None:
            attrs[DB_STORED_PROCEDURE_NAME] = stored_procedure_name

        if retry_attempts is not None and retry_attempts > 0:
            attrs[REDIS_CLIENT_OPERATION_RETRY_ATTEMPTS] = retry_attempts

        if is_blocking is not None:
            attrs[REDIS_CLIENT_OPERATION_BLOCKING] = is_blocking

        return attrs

    @staticmethod
    def build_connection_attributes(
            pool_name: str,
            connection_state: Optional[ConnectionState] = None,
            is_pubsub: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Build attributes for connection pool metrics.

        Args:
            pool_name: Unique connection pool name
            connection_state: Connection state ('idle' or 'used')
            is_pubsub: Whether this is a PubSub connection

        Returns:
            Dictionary of connection pool attributes
        """
        attrs: Dict[str, Any] = AttributeBuilder.build_base_attributes()
        attrs[DB_CLIENT_CONNECTION_POOL_NAME] = pool_name

        if connection_state is not None:
            attrs[DB_CLIENT_CONNECTION_STATE] = connection_state.value

        if is_pubsub is not None:
            attrs[REDIS_CLIENT_CONNECTION_PUBSUB] = is_pubsub

        return attrs

    @staticmethod
    def build_error_attributes(
            error_type: Optional[Exception] = None,
            is_internal: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Build error attributes.

        Args:
            is_internal: Whether the error is internal (e.g., timeout, network error)
            error_type: The exception that occurred

        Returns:
            Dictionary of error attributes
        """
        attrs: Dict[str, Any] = {}

        if error_type is not None:
            attrs[ERROR_TYPE] = AttributeBuilder.extract_error_type(error_type)

            if hasattr(error_type, "status_code") and error_type.status_code is not None:
                attrs[DB_RESPONSE_STATUS_CODE] = error_type.status_code
            else:
                attrs[DB_RESPONSE_STATUS_CODE] = "error"

        if is_internal is not None:
            attrs[REDIS_CLIENT_ERROR_INTERNAL] = is_internal

        return attrs

    @staticmethod
    def build_pubsub_message_attributes(
            direction: PubSubDirection,
            channel: Optional[str] = None,
            sharded: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Build attributes for a PubSub message.

        Args:
            direction: Message direction ('publish' or 'receive')
            channel: Pub/Sub channel name
            sharded: True if sharded Pub/Sub channel

        Returns:
            Dictionary of PubSub message attributes
        """
        attrs: Dict[str, Any] = AttributeBuilder.build_base_attributes()
        attrs[REDIS_CLIENT_PUBSUB_MESSAGE_DIRECTION] = direction.value

        if channel is not None:
            attrs[REDIS_CLIENT_PUBSUB_CHANNEL] = channel

        if sharded is not None:
            attrs[REDIS_CLIENT_PUBSUB_SHARDED] = sharded

        return attrs

    @staticmethod
    def build_streaming_attributes(
            stream_name: Optional[str] = None,
            consumer_group: Optional[str] = None,
            consumer_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Build attributes for a streaming operation.

        Args:
            stream_name: Name of the stream
            consumer_group: Name of the consumer group
            consumer_name: Name of the consumer

        Returns:
            Dictionary of streaming attributes
        """
        attrs: Dict[str, Any] = AttributeBuilder.build_base_attributes()

        if stream_name is not None:
            attrs[REDIS_CLIENT_STREAM_NAME] = stream_name

        if consumer_group is not None:
            attrs[REDIS_CLIENT_CONSUMER_GROUP] = consumer_group

        if consumer_name is not None:
            attrs[REDIS_CLIENT_CONSUMER_NAME] = consumer_name

        return attrs


    @staticmethod
    def extract_error_type(exception: Exception) -> str:
        """
        Extract error type from an exception.

        Args:
            exception: The exception that occurred

        Returns:
            Error type string (exception class name)
        """

        if hasattr(exception, "error_type"):
            return repr(exception)
        else:
            return f"other:{type(exception).__name__}"

    @staticmethod
    def build_pool_name(
            server_address: str,
            server_port: int,
            db_namespace: int = 0,
    ) -> str:
        """
        Build a unique connection pool name.

        Args:
            server_address: Redis server address
            server_port: Redis server port
            db_namespace: Redis database index

        Returns:
            Unique pool name in format "address:port/db"
        """
        return f"{server_address}:{server_port}/{db_namespace}"