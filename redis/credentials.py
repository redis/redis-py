from abc import ABC, abstractmethod
from typing import Optional, Tuple, Union, Callable, Any


class CredentialProvider:
    """
    Credentials Provider.
    """

    def get_credentials(self) -> Union[Tuple[str], Tuple[str, str]]:
        raise NotImplementedError("get_credentials must be implemented")


class StreamingCredentialProvider(CredentialProvider, ABC):
    @abstractmethod
    def on_next(self, callback: Callable[[Any], None]):
        pass

    @abstractmethod
    def is_streaming(self) -> bool:
        pass



class UsernamePasswordCredentialProvider(CredentialProvider):
    """
    Simple implementation of CredentialProvider that just wraps static
    username and password.
    """

    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        self.username = username or ""
        self.password = password or ""

    def get_credentials(self):
        if self.username:
            return self.username, self.password
        return (self.password,)
