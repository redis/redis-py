from unittest import mock

try:
    mock.AsyncMock
except AttributeError:
    import mock
