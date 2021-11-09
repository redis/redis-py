from json import JSONEncoder, JSONDecoder
from redis.exceptions import ModuleError


class RedisModuleCommands:
    """This class contains the wrapper functions to bring supported redis
    modules into the command namepsace.
    """

    def json(self, encoder=JSONEncoder(), decoder=JSONDecoder()):
        """Access the json namespace, providing support for redis json."""
        try:
            modversion = self.loaded_modules['rejson']
        except IndexError:
            raise ModuleError("rejson is not a loaded in the redis instance.")

        from .json import JSON
        jj = JSON(
                client=self,
                version=modversion,
                encoder=encoder,
                decoder=decoder)
        return jj

    def ft(self, index_name="idx"):
        """Access the search namespace, providing support for redis search."""
        try:
            modversion = self.loaded_modules['search']
        except IndexError:
            raise ModuleError("search is not a loaded in the redis instance.")

        from .search import Search
        s = Search(client=self, version=modversion, index_name=index_name)
        return s

    def ts(self, index_name="idx"):
        """Access the timeseries namespace, providing support for
        redis timeseries data.
        """
        try:
            modversion = self.loaded_modules['timeseries']
        except IndexError:
            raise ModuleError("timeseries is not a loaded in "
                              "the redis instance.")

        from .timeseries import TimeSeries
        s = TimeSeries(client=self, version=modversion, index_name=index_name)
        return s

    def bf(self):
        """Access the bloom namespace."""
        try:
            modversion = self.loaded_modules['bf']
        except IndexError:
            raise ModuleError("bloom is not a loaded in "
                              "the redis instance.")

        from .bf import BFBloom
        bf = BFBloom(client=self, version=modversion)
        return bf

    def cf(self):
        """Access the bloom namespace."""
        try:
            modversion = self.loaded_modules['bf']
        except IndexError:
            raise ModuleError("bloom is not a loaded in "
                              "the redis instance.")

        from .bf import CFBloom
        cf = CFBloom(client=self, version=modversion)
        return cf

    def cms(self):
        """Access the bloom namespace."""
        try:
            modversion = self.loaded_modules['bf']
        except IndexError:
            raise ModuleError("bloom is not a loaded in "
                              "the redis instance.")

        from .bf import CMSBloom
        cms = CMSBloom(client=self, version=modversion)
        return cms

    def topk(self):
        """Access the bloom namespace."""
        try:
            modversion = self.loaded_modules['bf']
        except IndexError:
            raise ModuleError("bloom is not a loaded in "
                              "the redis instance.")

        from .bf import TOPKBloom
        topk = TOPKBloom(client=self, version=modversion)
        return topk

    def tdigest(self):
        """Access the bloom namespace."""
        try:
            modversion = self.loaded_modules['bf']
        except IndexError:
            raise ModuleError("bloom is not a loaded in "
                              "the redis instance.")

        from .bf import TDigestBloom
        tdigest = TDigestBloom(client=self, version=modversion)
        return tdigest
