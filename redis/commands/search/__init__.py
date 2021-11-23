from .commands import SearchCommands


class Search(SearchCommands):
    """
    Create a client for talking to search.
    It abstracts the API of the module and lets you just use the engine.
    """

    class BatchIndexer(object):
        """
        A batch indexer allows you to automatically batch
        document indexing in pipelines, flushing it every N documents.
        """

        def __init__(self, client, chunk_size=1000):

            self.client = client
            self.execute_command = client.execute_command
            self.pipeline = client.pipeline(transaction=False, shard_hint=None)
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
            **fields
        ):
            """
            Add a document to the batch query
            """
            self.client._add_document(
                doc_id,
                conn=self.pipeline,
                nosave=nosave,
                score=score,
                payload=payload,
                replace=replace,
                partial=partial,
                no_create=no_create,
                **fields
            )
            self.current_chunk += 1
            self.total += 1
            if self.current_chunk >= self.chunk_size:
                self.commit()

        def add_document_hash(
            self,
            doc_id,
            score=1.0,
            replace=False,
        ):
            """
            Add a hash to the batch query
            """
            self.client._add_document_hash(
                doc_id,
                conn=self.pipeline,
                score=score,
                replace=replace,
            )
            self.current_chunk += 1
            self.total += 1
            if self.current_chunk >= self.chunk_size:
                self.commit()

        def commit(self):
            """
            Manually commit and flush the batch indexing query
            """
            self.pipeline.execute()
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
        self.pipeline = client.pipeline
