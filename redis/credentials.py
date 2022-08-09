from typing import Callable, Optional


class CredentialsProvider:
    def __init__(
        self,
        username: str = "",
        password: str = "",
        supplier: Optional[Callable] = None,
        *args,
        **kwargs,
    ):
        """
        Initialize a new Credentials Provider.
        :param supplier: a supplier function that returns the username and password.
                         def supplier(arg1, arg2, ...) -> (username, password)
                         For examples see examples/connection_examples.ipynb
        :param args: arguments to pass to the supplier function
        :param kwargs: keyword arguments to pass to the supplier function
        """
        self._username = "" if username is None else username
        self._password = "" if password is None else password
        self.supplier = supplier
        self.args = args
        self.kwargs = kwargs

    def get_credentials(self):
        if self.supplier:
            self.username, self.password = self.supplier(*self.args, **self.kwargs)
        return self._username, self._password

    @property
    def password(self):
        if self.supplier and not self._password:
            self.username, self.password = self.supplier(*self.args, **self.kwargs)
        return self._password

    @password.setter
    def password(self, value):
        self._password = value

    @property
    def username(self):
        if self.supplier and not self._username:
            self.username, self.password = self.supplier(*self.args, **self.kwargs)
        return self._username

    @username.setter
    def username(self, value):
        self._username = value
