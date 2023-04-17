import glob
import io
import importlib
import sys

import redis
import pytest


def run_example(context, name):
    mocked_stdout = io.StringIO()
    context.setattr(sys, "stdout", mocked_stdout)

    importlib.import_module(name=name)
    sys.modules.pop(name, None)
    return mocked_stdout.getvalue()


def example_files():
    return list(set(glob.glob("*.py")) - set(glob.glob("test*.py")))


@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    # Before a test we do:
    r = redis.Redis()
    r.flushdb()

    yield  # Test is executed here

    # After the test we can do additional clean-ups if needed


@pytest.mark.parametrize("example_file", example_files())
def test_examples(monkeypatch: pytest.MonkeyPatch, example_file):
    with monkeypatch.context() as context:
        # Do advanced monkey patching in individual tests if needed
        # or test output:
        # >>> assert "True\nbar\n" == run_example(context, "set_get")
        run_example(context, example_file[:-3])
