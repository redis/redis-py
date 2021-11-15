from json import JSONEncoder, JSONDecoder
from redis.exceptions import ModuleError


class RedisModuleCommands:
    """This class contains the wrapper functions to bring supported redis
    modules into the command namepsace.
    """

    def json(self, encoder=JSONEncoder(), decoder=JSONDecoder()):
        """Access the json namespace, providing support for redis json.
        """
        if 'JSON.SET' not in self.__commands__:
            raise ModuleError("redisjson is not loaded in redis. "
                              "For more information visit "
                              "https://redisjson.io/")

        from .json import JSON
        jj = JSON(
                client=self,
                encoder=encoder,
                decoder=decoder)
        return jj

    def ft(self, index_name="idx"):
        """Access the search namespace, providing support for redis search.
        """
        if 'FT.INFO' not in self.__commands__:
            raise ModuleError("redisearch is not loaded in redis. "
                              "For more information visit "
                              "https://redisearch.io/")

        from .search import Search
        s = Search(client=self, index_name=index_name)
        return s

    def ts(self):
        """Access the timeseries namespace, providing support for
        redis timeseries data.
        """
        if 'TS.INFO' not in self.__commands__:
            raise ModuleError("reditimeseries is not loaded in redis. "
                              "For more information visit "
                              "https://redistimeseries.io/")

        from .timeseries import TimeSeries
        s = TimeSeries(client=self)
        return s
