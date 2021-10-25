from json import JSONEncoder, JSONDecoder
from redis.exceptions import ModuleError


class RedisModuleCommands:
    """This class contains the wrapper functions to bring supported redis
    modules into the command namepsace.
    """

    def json(self, encoder=JSONEncoder(), decoder=JSONDecoder()):
        """Access the json namespace, providing support for redis json."""
        if 'rejson' not in self.loaded_modules:
            raise ModuleError("rejson is not a loaded in the redis instance.")

        from .json import JSON
        jj = JSON(client=self, encoder=encoder, decoder=decoder)
        return jj

    def ft(self, index_name="idx"):
        """Access the search namespace, providing support for redis search."""
        if 'search' not in self.loaded_modules:
            raise ModuleError("search is not a loaded in the redis instance.")

        from .search import Search
        s = Search(client=self, index_name=index_name)
        return s
