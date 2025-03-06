# Command examples for redis.io

## How to add an example

Create regular python file in the current folder with meaningful name. It makes sense prefix example files with
command category (e.g. string, set, list, hash, etc) to make navigation in the folder easier. Files ending in *.py*
are automatically run by the test suite.

### Special markup

See https://github.com/redis-stack/redis-stack-website#readme for more details.

## How to test examples

Examples are standalone python scripts, committed to the *doctests* directory. These scripts assume that the
```doctests/requirements.txt``` and ```dev_requirements.txt``` from this repository have been installed, as per below.

```bash
pip install -r dev_requirements.txt
pip uninstall -y redis  # uninstall Redis package installed via redis-entraid
pip install -r doctests/requirements.txt
```

Note - the CI process, runs linters against the examples. Assuming
the requirements above have been installed you can run ```ruff check yourfile.py``` and ```ruff format yourfile.py```
locally to validate the linting, prior to CI.

Just include necessary assertions in the example file and run
```bash
sh doctests/run_examples.sh
```
to test all examples in the current folder.
