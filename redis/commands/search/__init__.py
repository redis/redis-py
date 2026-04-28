from typing import Literal

from redis.client import Pipeline as RedisPipeline

from ...asyncio.client import Pipeline as AsyncioPipeline
from ..helpers import (
    apply_module_callbacks,
    get_legacy_responses,
    get_protocol_version,
)
from .commands import (
    AGGREGATE_CMD,
    CONFIG_CMD,
    HYBRID_CMD,
    INFO_CMD,
    PROFILE_CMD,
    SEARCH_CMD,
    SPELLCHECK_CMD,
    SYNDUMP_CMD,
    AsyncSearchCommands,
    SearchCommands,
)
from .profile_information import ProfileInformation


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
        self._RESP2_MODULE_CALLBACKS = {
            INFO_CMD: self._parse_info,
            SEARCH_CMD: self._parse_search,
            HYBRID_CMD: self._parse_hybrid_search,
            AGGREGATE_CMD: self._parse_aggregate,
            PROFILE_CMD: self._parse_profile,
            SPELLCHECK_CMD: self._parse_spellcheck,
            CONFIG_CMD: self._parse_config_get,
            SYNDUMP_CMD: self._parse_syndump,
        }
        self._RESP3_MODULE_CALLBACKS = {
            PROFILE_CMD: lambda r, **kwargs: ProfileInformation(r),
        }
        self._RESP2_UNIFIED_MODULE_CALLBACKS = dict(self._RESP2_MODULE_CALLBACKS)
        self._RESP3_UNIFIED_MODULE_CALLBACKS = dict(self._RESP3_MODULE_CALLBACKS)
        self._RESP3_TO_RESP2_LEGACY_MODULE_CALLBACKS = {}

        self._MODULE_CALLBACKS = apply_module_callbacks(
            get_protocol_version(self.client),
            get_legacy_responses(self.client),
            common={},
            resp2=self._RESP2_MODULE_CALLBACKS,
            resp3=self._RESP3_MODULE_CALLBACKS,
            resp2_unified=self._RESP2_UNIFIED_MODULE_CALLBACKS,
            resp3_unified=self._RESP3_UNIFIED_MODULE_CALLBACKS,
            resp3_to_resp2_legacy=self._RESP3_TO_RESP2_LEGACY_MODULE_CALLBACKS,
        )

    def pipeline(self, transaction=True, shard_hint=None):
        """Creates a pipeline for the SEARCH module, that can be used for executing
        SEARCH commands, as well as classic core commands.
        """
        p = Pipeline(
            connection_pool=self.client.connection_pool,
            response_callbacks=self._MODULE_CALLBACKS,
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
            response_callbacks=self._MODULE_CALLBACKS,
            transaction=transaction,
            shard_hint=shard_hint,
        )
        p.index_name = self.index_name
        return p


class Pipeline(SearchCommands, RedisPipeline):
    """Pipeline for the module."""

    _is_async_client: Literal[False] = False


class AsyncPipeline(AsyncSearchCommands, AsyncioPipeline, Pipeline):
    """AsyncPipeline for the module."""

    _is_async_client: Literal[True] = True
