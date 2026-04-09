from functools import partial
from typing import Literal

from redis.client import Pipeline as RedisPipeline

from ...asyncio.client import Pipeline as AsyncioPipeline
from .commands import (
    AGGREGATE_CMD,
    CURSOR_CMD,
    AsyncSearchCommands,
    SearchCommands,
)


class Search(SearchCommands):
    """
    Create a client for talking to search.
    It abstracts the API of the module and lets you just use the engine.
    """

    class BatchIndexer:
        """
        A batch indexer allows you to automatically batch
        document indexing in pipelines, flushing it every N documents.
        """

        def __init__(self, client, chunk_size=1000):
            self.client = client
            self.execute_command = client.execute_command
            self._pipeline = client.pipeline(transaction=False, shard_hint=None)
            self.total = 0
            self.chunk_size = chunk_size
            self.current_chunk = 0

        def __del__(self):
            if self.current_chunk:
                self.commit()

        def add_document(
            self,
            doc_id,
            nosave=False,
            score=1.0,
            payload=None,
            replace=False,
            partial=False,
            no_create=False,
            **fields,
        ):
            """
            Add a document to the batch query
            """
            self.client._add_document(
                doc_id,
                conn=self._pipeline,
                nosave=nosave,
                score=score,
                payload=payload,
                replace=replace,
                partial=partial,
                no_create=no_create,
                **fields,
            )
            self.current_chunk += 1
            self.total += 1
            if self.current_chunk >= self.chunk_size:
                self.commit()

        def add_document_hash(self, doc_id, score=1.0, replace=False):
            """
            Add a hash to the batch query
            """
            self.client._add_document_hash(
                doc_id, conn=self._pipeline, score=score, replace=replace
            )
            self.current_chunk += 1
            self.total += 1
            if self.current_chunk >= self.chunk_size:
                self.commit()

        def commit(self):
            """
            Manually commit and flush the batch indexing query
            """
            self._pipeline.execute()
            self.current_chunk = 0

    def __init__(self, client, index_name="idx"):
        """
        Create a new Client for the given index_name.
        The default name is `idx`

        If conn is not None, we employ an already existing redis connection
        """
        self.client = client
        self.index_name = index_name
        self.execute_command = client.execute_command
        self._pipeline = client.pipeline
        self._init_module_callbacks()

    def pipeline(self, transaction=True, shard_hint=None):
        """Creates a pipeline for the SEARCH module, that can be used for executing
        SEARCH commands, as well as classic core commands.
        """
        p = Pipeline(
            connection_pool=self.client.connection_pool,
            response_callbacks=self.client.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
        )
        p.index_name = self.index_name
        return p


class AsyncSearch(Search, AsyncSearchCommands):
    class BatchIndexer(Search.BatchIndexer):
        """
        A batch indexer allows you to automatically batch
        document indexing in pipelines, flushing it every N documents.
        """

        async def add_document(
            self,
            doc_id,
            nosave=False,
            score=1.0,
            payload=None,
            replace=False,
            partial=False,
            no_create=False,
            **fields,
        ):
            """
            Add a document to the batch query
            """
            self.client._add_document(
                doc_id,
                conn=self._pipeline,
                nosave=nosave,
                score=score,
                payload=payload,
                replace=replace,
                partial=partial,
                no_create=no_create,
                **fields,
            )
            self.current_chunk += 1
            self.total += 1
            if self.current_chunk >= self.chunk_size:
                await self.commit()

        async def commit(self):
            """
            Manually commit and flush the batch indexing query
            """
            await self._pipeline.execute()
            self.current_chunk = 0

    def pipeline(self, transaction=True, shard_hint=None):
        """Creates a pipeline for the SEARCH module, that can be used for executing
        SEARCH commands, as well as classic core commands.
        """
        p = AsyncPipeline(
            connection_pool=self.client.connection_pool,
            response_callbacks=self.client.response_callbacks,
            transaction=transaction,
            shard_hint=shard_hint,
        )
        p.index_name = self.index_name
        return p


class Pipeline(SearchCommands, RedisPipeline):
    """Pipeline for the module."""

    _is_async_client: Literal[False] = False

    def __init__(self, connection_pool, response_callbacks, transaction, shard_hint):
        # Copy response_callbacks so search-specific entries don't pollute
        # the shared client dict.
        super().__init__(
            connection_pool, dict(response_callbacks), transaction, shard_hint
        )
        self._init_module_callbacks()
        # Register search-specific response callbacks so that the standard
        # pipeline callback mechanism (and future cluster pipeline support)
        # can apply them automatically — no execute() override needed.
        # Derive the set of commands from the module callback maps built by
        # _init_module_callbacks() so new commands are picked up automatically.
        # _parse_results dispatches to the correct protocol-specific callback
        # at runtime, so we only need the union of command names here.
        for cmd in (
            self._RESP2_MODULE_CALLBACKS.keys() | self._RESP3_MODULE_CALLBACKS.keys()
        ):
            self.response_callbacks[cmd] = partial(self._parse_results, cmd)
        # CURSOR_CMD reuses the AGGREGATE parser but isn't in the maps.
        self.response_callbacks[CURSOR_CMD] = partial(
            self._parse_results, AGGREGATE_CMD
        )

    @property
    def client(self):
        """Return self so get_protocol_version() can access connection_pool."""
        return self


class AsyncPipeline(AsyncSearchCommands, AsyncioPipeline, Pipeline):
    """AsyncPipeline for the module."""

    _is_async_client: Literal[True] = True

    def __init__(self, connection_pool, response_callbacks, transaction, shard_hint):
        # AsyncioPipeline.__init__ is next in MRO and won't chain to our
        # sync Pipeline.__init__, so we must set up callbacks here directly.
        super().__init__(
            connection_pool, dict(response_callbacks), transaction, shard_hint
        )
        self._init_module_callbacks()
        for cmd in (
            self._RESP2_MODULE_CALLBACKS.keys() | self._RESP3_MODULE_CALLBACKS.keys()
        ):
            self.response_callbacks[cmd] = partial(self._parse_results, cmd)
        self.response_callbacks[CURSOR_CMD] = partial(
            self._parse_results, AGGREGATE_CMD
        )

    @property
    def client(self):
        """Return self so get_protocol_version() can access connection_pool.

        Must be redefined here because redis.asyncio.client.Redis.client (a plain
        method) appears earlier in the MRO than Pipeline.client (a property) and
        would shadow it.
        """
        return self
