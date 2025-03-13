import asyncio
import threading
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, Union

from redis.auth.token import TokenInterface
from redis.credentials import CredentialProvider, StreamingCredentialProvider


class EventListenerInterface(ABC):
    """
    Represents a listener for given event object.
    """

    @abstractmethod
    def listen(self, event: object):
        pass


class AsyncEventListenerInterface(ABC):
    """
    Represents an async listener for given event object.
    """

    @abstractmethod
    async def listen(self, event: object):
        pass


class EventDispatcherInterface(ABC):
    """
    Represents a dispatcher that dispatches events to listeners
    associated with given event.
    """

    @abstractmethod
    def dispatch(self, event: object):
        pass

    @abstractmethod
    async def dispatch_async(self, event: object):
        pass


class EventException(Exception):
    """
    Exception wrapper that adds an event object into exception context.
    """

    def __init__(self, exception: Exception, event: object):
        self.exception = exception
        self.event = event
        super().__init__(exception)


class EventDispatcher(EventDispatcherInterface):
    # TODO: Make dispatcher to accept external mappings.
    def __init__(self):
        """
        Mapping should be extended for any new events or listeners to be added.
        """
        self._event_listeners_mapping = {
            AfterConnectionReleasedEvent: [
                ReAuthConnectionListener(),
            ],
            AfterPooledConnectionsInstantiationEvent: [
                RegisterReAuthForPooledConnections()
            ],
            AfterSingleConnectionInstantiationEvent: [
                RegisterReAuthForSingleConnection()
            ],
            AfterPubSubConnectionInstantiationEvent: [RegisterReAuthForPubSub()],
            AfterAsyncClusterInstantiationEvent: [RegisterReAuthForAsyncClusterNodes()],
            AsyncAfterConnectionReleasedEvent: [
                AsyncReAuthConnectionListener(),
            ],
        }

    def dispatch(self, event: object):
        listeners = self._event_listeners_mapping.get(type(event))

        for listener in listeners:
            listener.listen(event)

    async def dispatch_async(self, event: object):
        listeners = self._event_listeners_mapping.get(type(event))

        for listener in listeners:
            await listener.listen(event)


class AfterConnectionReleasedEvent:
    """
    Event that will be fired before each command execution.
    """

    def __init__(self, connection):
        self._connection = connection

    @property
    def connection(self):
        return self._connection


class AsyncAfterConnectionReleasedEvent(AfterConnectionReleasedEvent):
    pass


class ClientType(Enum):
    SYNC = ("sync",)
    ASYNC = ("async",)


class AfterPooledConnectionsInstantiationEvent:
    """
    Event that will be fired after pooled connection instances was created.
    """

    def __init__(
        self,
        connection_pools: List,
        client_type: ClientType,
        credential_provider: Optional[CredentialProvider] = None,
    ):
        self._connection_pools = connection_pools
        self._client_type = client_type
        self._credential_provider = credential_provider

    @property
    def connection_pools(self):
        return self._connection_pools

    @property
    def client_type(self) -> ClientType:
        return self._client_type

    @property
    def credential_provider(self) -> Union[CredentialProvider, None]:
        return self._credential_provider


class AfterSingleConnectionInstantiationEvent:
    """
    Event that will be fired after single connection instances was created.

    :param connection_lock: For sync client thread-lock should be provided,
    for async asyncio.Lock
    """

    def __init__(
        self,
        connection,
        client_type: ClientType,
        connection_lock: Union[threading.Lock, asyncio.Lock],
    ):
        self._connection = connection
        self._client_type = client_type
        self._connection_lock = connection_lock

    @property
    def connection(self):
        return self._connection

    @property
    def client_type(self) -> ClientType:
        return self._client_type

    @property
    def connection_lock(self) -> Union[threading.Lock, asyncio.Lock]:
        return self._connection_lock


class AfterPubSubConnectionInstantiationEvent:
    def __init__(
        self,
        pubsub_connection,
        connection_pool,
        client_type: ClientType,
        connection_lock: Union[threading.Lock, asyncio.Lock],
    ):
        self._pubsub_connection = pubsub_connection
        self._connection_pool = connection_pool
        self._client_type = client_type
        self._connection_lock = connection_lock

    @property
    def pubsub_connection(self):
        return self._pubsub_connection

    @property
    def connection_pool(self):
        return self._connection_pool

    @property
    def client_type(self) -> ClientType:
        return self._client_type

    @property
    def connection_lock(self) -> Union[threading.Lock, asyncio.Lock]:
        return self._connection_lock


class AfterAsyncClusterInstantiationEvent:
    """
    Event that will be fired after async cluster instance was created.

    Async cluster doesn't use connection pools,
    instead ClusterNode object manages connections.
    """

    def __init__(
        self,
        nodes: dict,
        credential_provider: Optional[CredentialProvider] = None,
    ):
        self._nodes = nodes
        self._credential_provider = credential_provider

    @property
    def nodes(self) -> dict:
        return self._nodes

    @property
    def credential_provider(self) -> Union[CredentialProvider, None]:
        return self._credential_provider


class ReAuthConnectionListener(EventListenerInterface):
    """
    Listener that performs re-authentication of given connection.
    """

    def listen(self, event: AfterConnectionReleasedEvent):
        event.connection.re_auth()


class AsyncReAuthConnectionListener(AsyncEventListenerInterface):
    """
    Async listener that performs re-authentication of given connection.
    """

    async def listen(self, event: AsyncAfterConnectionReleasedEvent):
        await event.connection.re_auth()


class RegisterReAuthForPooledConnections(EventListenerInterface):
    """
    Listener that registers a re-authentication callback for pooled connections.
    Required by :class:`StreamingCredentialProvider`.
    """

    def __init__(self):
        self._event = None

    def listen(self, event: AfterPooledConnectionsInstantiationEvent):
        if isinstance(event.credential_provider, StreamingCredentialProvider):
            self._event = event

            if event.client_type == ClientType.SYNC:
                event.credential_provider.on_next(self._re_auth)
                event.credential_provider.on_error(self._raise_on_error)
            else:
                event.credential_provider.on_next(self._re_auth_async)
                event.credential_provider.on_error(self._raise_on_error_async)

    def _re_auth(self, token):
        for pool in self._event.connection_pools:
            pool.re_auth_callback(token)

    async def _re_auth_async(self, token):
        for pool in self._event.connection_pools:
            await pool.re_auth_callback(token)

    def _raise_on_error(self, error: Exception):
        raise EventException(error, self._event)

    async def _raise_on_error_async(self, error: Exception):
        raise EventException(error, self._event)


class RegisterReAuthForSingleConnection(EventListenerInterface):
    """
    Listener that registers a re-authentication callback for single connection.
    Required by :class:`StreamingCredentialProvider`.
    """

    def __init__(self):
        self._event = None

    def listen(self, event: AfterSingleConnectionInstantiationEvent):
        if isinstance(
            event.connection.credential_provider, StreamingCredentialProvider
        ):
            self._event = event

            if event.client_type == ClientType.SYNC:
                event.connection.credential_provider.on_next(self._re_auth)
                event.connection.credential_provider.on_error(self._raise_on_error)
            else:
                event.connection.credential_provider.on_next(self._re_auth_async)
                event.connection.credential_provider.on_error(
                    self._raise_on_error_async
                )

    def _re_auth(self, token):
        with self._event.connection_lock:
            self._event.connection.send_command(
                "AUTH", token.try_get("oid"), token.get_value()
            )
            self._event.connection.read_response()

    async def _re_auth_async(self, token):
        async with self._event.connection_lock:
            await self._event.connection.send_command(
                "AUTH", token.try_get("oid"), token.get_value()
            )
            await self._event.connection.read_response()

    def _raise_on_error(self, error: Exception):
        raise EventException(error, self._event)

    async def _raise_on_error_async(self, error: Exception):
        raise EventException(error, self._event)


class RegisterReAuthForAsyncClusterNodes(EventListenerInterface):
    def __init__(self):
        self._event = None

    def listen(self, event: AfterAsyncClusterInstantiationEvent):
        if isinstance(event.credential_provider, StreamingCredentialProvider):
            self._event = event
            event.credential_provider.on_next(self._re_auth)
            event.credential_provider.on_error(self._raise_on_error)

    async def _re_auth(self, token: TokenInterface):
        for key in self._event.nodes:
            await self._event.nodes[key].re_auth_callback(token)

    async def _raise_on_error(self, error: Exception):
        raise EventException(error, self._event)


class RegisterReAuthForPubSub(EventListenerInterface):
    def __init__(self):
        self._connection = None
        self._connection_pool = None
        self._client_type = None
        self._connection_lock = None
        self._event = None

    def listen(self, event: AfterPubSubConnectionInstantiationEvent):
        if isinstance(
            event.pubsub_connection.credential_provider, StreamingCredentialProvider
        ) and event.pubsub_connection.get_protocol() in [3, "3"]:
            self._event = event
            self._connection = event.pubsub_connection
            self._connection_pool = event.connection_pool
            self._client_type = event.client_type
            self._connection_lock = event.connection_lock

            if self._client_type == ClientType.SYNC:
                self._connection.credential_provider.on_next(self._re_auth)
                self._connection.credential_provider.on_error(self._raise_on_error)
            else:
                self._connection.credential_provider.on_next(self._re_auth_async)
                self._connection.credential_provider.on_error(
                    self._raise_on_error_async
                )

    def _re_auth(self, token: TokenInterface):
        with self._connection_lock:
            self._connection.send_command(
                "AUTH", token.try_get("oid"), token.get_value()
            )
            self._connection.read_response()

        self._connection_pool.re_auth_callback(token)

    async def _re_auth_async(self, token: TokenInterface):
        async with self._connection_lock:
            await self._connection.send_command(
                "AUTH", token.try_get("oid"), token.get_value()
            )
            await self._connection.read_response()

        await self._connection_pool.re_auth_callback(token)

    def _raise_on_error(self, error: Exception):
        raise EventException(error, self._event)

    async def _raise_on_error_async(self, error: Exception):
        raise EventException(error, self._event)
