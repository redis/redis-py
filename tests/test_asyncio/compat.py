try:
    from contextlib import aclosing
except ImportError:
    import contextlib

    @contextlib.asynccontextmanager
    async def aclosing(thing):
        try:
            yield thing
        finally:
            await thing.aclose()
