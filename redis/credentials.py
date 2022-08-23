from typing import Callable, Optional, Union


class CredentialProvider:
    def __init__(
        self,
        username: Union[str, None] = "",
        password: Union[str, None] = "",
        supplier: Optional[Callable] = None,
        *supplier_args,
        **supplier_kwargs,
    ):
        """
        Initialize a new Credentials Provider.
        :param supplier: a supplier function that returns the username and password.
                         def supplier(self, arg1, arg2, ...) -> (username, password)
                         See examples/connection_examples.ipynb
        :param supplier_args: arguments to pass to the supplier function
        :param supplier_kwargs: keyword arguments to pass to the supplier function
        """
        self._username = "" if username is None else username
        self._password = "" if password is None else password
        self.supplier = supplier
        self.supplier_args = supplier_args
        self.supplier_kwargs = supplier_kwargs

    def get_credentials(self):
        if self.supplier is not None:
            self.username, self.password = self.supplier(
                self, *self.supplier_args, **self.supplier_kwargs
            )

        return (self._username, self._password) if self._username else (self._password,)

    @property
    def password(self):
        if self.supplier is not None and not self._password:
            self.username, self.password = self.supplier(
                self, *self.supplier_args, **self.supplier_kwargs
            )
        return self._password

    @password.setter
    def password(self, value):
        self._password = value

    @property
    def username(self):
        if self.supplier is not None and not self._username:
            self.username, self.password = self.supplier(
                self, *self.supplier_args, **self.supplier_kwargs
            )
        return self._username

    @username.setter
    def username(self, value):
        self._username = value


class StaticCredentialProvider(CredentialProvider):
    """
    Simple implementation of CredentialProvider that just wraps static
    username and password.
    """

    def __init__(
        self, username: Union[str, None] = "", password: Union[str, None] = ""
    ):
        super().__init__(
            username=username,
            password=password,
            credential_provider=lambda self: (self.username, self.password),
        )
