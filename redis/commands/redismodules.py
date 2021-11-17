from json import JSONEncoder, JSONDecoder


class RedisModuleCommands:
    """This class contains the wrapper functions to bring supported redis
    modules into the command namepsace.
    """

    def json(self, encoder=JSONEncoder(), decoder=JSONDecoder()):
        """Access the json namespace, providing support for redis json.
        """

        from .json import JSON
        jj = JSON(
                client=self,
                encoder=encoder,
                decoder=decoder)
        return jj

    def ft(self, index_name="idx"):
        """Access the search namespace, providing support for redis search.
        """

        from .search import Search
        s = Search(client=self, index_name=index_name)
        return s

    def ts(self):
        """Access the timeseries namespace, providing support for
        redis timeseries data.
        """

        from .timeseries import TimeSeries
        s = TimeSeries(client=self)
        return s
