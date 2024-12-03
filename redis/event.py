from abc import ABC, abstractmethod
from typing import List, Union, Optional

from redis.credentials import StreamingCredentialProvider, CredentialProvider


class EventListenerInterface(ABC):
    """
    Represents a listener for given event object.
    """
    @abstractmethod
    def listen(self, event: object):
        pass


class EventDispatcherInterface(ABC):
    """
    Represents a dispatcher that dispatches events to listeners associated with given event.
    """
    @abstractmethod
    def dispatch(self, event: object):
        pass


class EventDispatcher(EventDispatcherInterface):

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
            ]
        }

    def dispatch(self, event: object):
        listeners = self._event_listeners_mapping.get(type(event))

        for listener in listeners:
            listener.listen(event)


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


class AfterPooledConnectionsInstantiationEvent:
    """
    Event that will be fired after pooled connection instances was created.
    """
    def __init__(
            self,
            connection_pools,
            credential_provider: Optional[CredentialProvider] = None,
    ):
        self._connection_pools = connection_pools
        self._credential_provider = credential_provider

    @property
    def connection_pools(self):
        return self._connection_pools

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
        if self._current_cred is None:
            self._current_cred = event.initial_cred

        credentials = event.credential_provider.get_credentials()

        if hash(credentials) != self._current_cred:
            self._current_cred = hash(credentials)
            event.connection.send_command('AUTH', credentials[0], credentials[1])
            event.connection.read_response()


class RegisterReAuthForPooledConnections(EventListenerInterface):
    def __init__(self):
        self._event = None

    """
    Listener that registers a re-authentication callback for pooled connections.
    Required by :class:`StreamingCredentialProvider`.
    """
    def listen(self, event: AfterPooledConnectionsInstantiationEvent):
        if isinstance(event.credential_provider, StreamingCredentialProvider):
            self._event = event
            event.credential_provider.on_next(self._re_auth)

    def _re_auth(self, token):
        for pool in self._event.connection_pools:
            pool.re_auth_callback(token)