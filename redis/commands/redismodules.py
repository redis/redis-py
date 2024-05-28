from json import JSONDecoder, JSONEncoder
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .bf import BFBloom, CFBloom, CMSBloom, TDigestBloom, TOPKBloom
    from .graph import AsyncGraph, Graph
    from .json import JSON
    from .search import AsyncSearch, Search
    from .timeseries import TimeSeries


class RedisModuleCommands:
    """This class contains the wrapper functions to bring supported redis
    modules into the command namespace.
    """

    def json(
        self,
        encoder: JSONEncoder = JSONEncoder(),
        decoder: JSONDecoder = JSONDecoder(),
    ) -> "JSON":
        """Access the json namespace, providing support for redis json."""

        from .json import JSON

        jj = JSON(client=self, encoder=encoder, decoder=decoder)
        return jj

    def ft(self, index_name: str = "idx") -> "Search":
        """Access the search namespace, providing support for redis search."""

        from .search import Search

        s = Search(client=self, index_name=index_name)
        return s

    def ts(self) -> "TimeSeries":
        """Access the timeseries namespace, providing support for
        redis timeseries data.
        """

        from .timeseries import TimeSeries

        s = TimeSeries(client=self)
        return s

    def bf(self) -> "BFBloom":
        """Access the bloom namespace."""

        from .bf import BFBloom

        bf = BFBloom(client=self)
        return bf

    def cf(self) -> "CFBloom":
        """Access the bloom namespace."""

        from .bf import CFBloom

        cf = CFBloom(client=self)
        return cf

    def cms(self) -> "CMSBloom":
        """Access the bloom namespace."""

        from .bf import CMSBloom

        cms = CMSBloom(client=self)
        return cms

    def topk(self) -> "TOPKBloom":
        """Access the bloom namespace."""

        from .bf import TOPKBloom

        topk = TOPKBloom(client=self)
        return topk

    def tdigest(self) -> "TDigestBloom":
        """Access the bloom namespace."""

        from .bf import TDigestBloom

        tdigest = TDigestBloom(client=self)
        return tdigest

    def graph(self, index_name: str = "idx") -> "Graph":
        """Access the graph namespace, providing support for
        redis graph data.
        """

        from .graph import Graph

        g = Graph(client=self, name=index_name)
        return g


class AsyncRedisModuleCommands(RedisModuleCommands):
    def ft(self, index_name: str = "idx") -> "AsyncSearch":
        """Access the search namespace, providing support for redis search."""

        from .search import AsyncSearch

        s = AsyncSearch(client=self, index_name=index_name)
        return s

    def graph(self, index_name: str = "idx") -> "AsyncGraph":
        """Access the graph namespace, providing support for
        redis graph data.
        """

        from .graph import AsyncGraph

        g = AsyncGraph(client=self, name=index_name)
        return g
