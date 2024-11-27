from abc import ABC, abstractmethod

from redis.credentials import StreamingCredentialProvider


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
