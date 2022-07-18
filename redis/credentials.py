class CredentialsProvider:
    def __init__(
        self,
        username: str = "",
        password: str = "",
        supplier: callable = None,
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
        self.username = username
        self.password = password
        self.supplier = supplier
        self.args = args
        self.kwargs = kwargs

    def get_credentials(self):
        if self.supplier:
            self.username, self.password = self.supplier(*self.args, **self.kwargs)
        if self.username:
            auth_args = (self.username, self.password or "")
        else:
            auth_args = (self.password,)
        return auth_args

    def get_password(self, call_supplier: bool = True):
        if call_supplier and self.supplier:
            self.username, self.password = self.supplier(*self.args, **self.kwargs)
        return self.password

    def get_username(self, call_supplier: bool = True):
        if call_supplier and self.supplier:
            self.username, self.password = self.supplier(*self.args, **self.kwargs)
        return self.username
