Instructions for AI agents contributing to redis-py

1. Use PEP604 for the type hints format
2. Follow the guidelines in .agent/sync_async_type_hints_overload_guide.md for adding type hints and overloads
3. Don't import types in functions, only in the top of the file (unless absolutely necessary)
3. Run the test suite and fix any test failures
4. Run linting and fix any issues