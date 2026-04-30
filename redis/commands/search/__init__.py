from typing import Literal

from redis.client import Pipeline as RedisPipeline

from ...asyncio.client import Pipeline as AsyncioPipeline
from ...utils import check_protocol_version
from ..helpers import get_legacy_responses, get_protocol_version
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
        # Copy the client's response_callbacks so search-specific entries
        # don't pollute the shared dict.
        super().__init__(
            connection_pool, dict(response_callbacks), transaction, shard_hint
        )
        self._init_module_callbacks()
        self._register_module_callbacks()

    def _register_module_callbacks(self):
        # Pipeline post-processing matches the pre-migration behavior:
        # legacy mode returns raw pipeline responses like v8.0.0b1.  The
        # default connection now uses RESP3 on the wire, so it gets a small
        # adapter for the old raw RESP2 pipeline shape; explicit RESP3 keeps
        # the previous native shape, with HYBRID's experimental normalizer.
        # Only ``legacy_responses=False`` registers the unified parsers that
        # post-process every response.
        protocol = get_protocol_version(self)
        if get_legacy_responses(self):
            if protocol is None:
                cmd_callbacks = self._RESP3_TO_RESP2_LEGACY_PIPELINE_CALLBACKS
            elif check_protocol_version(protocol, 3):
                cmd_callbacks = self._RESP3_MODULE_CALLBACKS
            else:
                cmd_callbacks = {}
        else:
            if check_protocol_version(protocol, 3):
                cmd_callbacks = self._RESP3_UNIFIED_MODULE_CALLBACKS
            else:
                cmd_callbacks = self._RESP2_UNIFIED_MODULE_CALLBACKS
        for cmd, cb in cmd_callbacks.items():
            self.response_callbacks[cmd] = cb
        # ``FT.CURSOR`` shares the AGGREGATE parser but isn't in the maps.
        agg_cb = cmd_callbacks.get(AGGREGATE_CMD)
        if agg_cb is not None:
            self.response_callbacks[CURSOR_CMD] = agg_cb

    @property
    def client(self):
        """Return self so ``get_protocol_version`` can read connection_pool."""
        return self


class AsyncPipeline(AsyncSearchCommands, AsyncioPipeline, Pipeline):
    """AsyncPipeline for the module."""

    _is_async_client: Literal[True] = True

    def __init__(self, connection_pool, response_callbacks, transaction, shard_hint):
        # ``AsyncioPipeline.__init__`` is next in MRO and won't chain to
        # the sync ``Pipeline.__init__``, so we set up callbacks here.
        super().__init__(
            connection_pool, dict(response_callbacks), transaction, shard_hint
        )
        self._init_module_callbacks()
        self._register_module_callbacks()

    @property
    def client(self):
        """Return self so ``get_protocol_version`` can read connection_pool.

        Redefined here because ``redis.asyncio.client.Redis.client`` (a
        plain method) appears earlier in the MRO than ``Pipeline.client``
        (a property) and would otherwise shadow it.
        """
        return self
