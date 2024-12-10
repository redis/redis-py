from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Union, Optional

from redis.credentials import StreamingCredentialProvider, CredentialProvider


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
    Represents a dispatcher that dispatches events to listeners associated with given event.
    """
    @abstractmethod
    def dispatch(self, event: object):
        pass

    @abstractmethod
    async def dispatch_async(self, event: object):
        pass


class EventDispatcher(EventDispatcherInterface):
    # TODO: Make dispatcher to accept external mappings.
    def __init__(self):
        """
        Mapping should be extended for any new events or listeners to be added.
        """
        self._event_listeners_mapping = {
            BeforeCommandExecutionEvent: [
                ReAuthBeforeCommandExecutionListener(),
            ],
            AfterPooledConnectionsInstantiationEvent: [
                RegisterReAuthForPooledConnections()
            ],
            AsyncBeforeCommandExecutionEvent: [
                AsyncReAuthBeforeCommandExecutionListener(),
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


class BeforeCommandExecutionEvent:
    """
    Event that will be fired before each command execution.
    """
    def __init__(self, command, initial_cred, connection, credential_provider: StreamingCredentialProvider):
        self._command = command
        self._initial_cred = initial_cred
        self._credential_provider = credential_provider
        self._connection = connection

    @property
    def command(self):
        return self._command

    @property
    def initial_cred(self):
        return self._initial_cred

    @property
    def connection(self):
        return self._connection

    @property
    def credential_provider(self) -> StreamingCredentialProvider:
        return self._credential_provider


class AsyncBeforeCommandExecutionEvent(BeforeCommandExecutionEvent):
    pass


class ClientType(Enum):
    SYNC = "sync",
    ASYNC = "async",


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


class ReAuthBeforeCommandExecutionListener(EventListenerInterface):
    """
    Listener that performs re-authentication (if needed) for StreamingCredentialProviders before command execution.
    """
    def __init__(self):
        self._current_cred = None

    def listen(self, event: BeforeCommandExecutionEvent):
        if event.command[0] == 'AUTH':
            return

        if self._current_cred is None:
            self._current_cred = event.initial_cred

        credentials = event.credential_provider.get_credentials()

        if hash(credentials) != self._current_cred:
            self._current_cred = hash(credentials)
            event.connection.send_command('AUTH', credentials[0], credentials[1])
            event.connection.read_response()


class AsyncReAuthBeforeCommandExecutionListener(AsyncEventListenerInterface):
    """
    Async listener that performs re-authentication (if needed) for StreamingCredentialProviders before command execution
    """
    def __init__(self):
        self._current_cred = None

    async def listen(self, event: AsyncBeforeCommandExecutionEvent):
        if self._current_cred is None:
            self._current_cred = event.initial_cred

        credentials = await event.credential_provider.get_credentials_async()

        if hash(credentials) != self._current_cred:
            self._current_cred = hash(credentials)
            await event.connection.send_command('AUTH', credentials[0], credentials[1])
            await event.connection.read_response()


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
            else:
                event.credential_provider.on_next(self._re_auth_async)

    def _re_auth(self, token):
        for pool in self._event.connection_pools:
            pool.re_auth_callback(token)

    async def _re_auth_async(self, token):
        for pool in self._event.connection_pools:
            await pool.re_auth_callback(token)
