# Command examples for redis.io

## How to add example

Create regular python file in the current folder with meaningful name. It makes sense prefix example files with 
command category (e.g. string, set, list, hash, etc) to make navigation in the folder easier.

### Special markup

See https://github.com/redis-stack/redis-stack-website#readme for more details.

## How to test example

Just include necessary assertions in the example file and run
```bash
pytest -v tests.py
```
to test all examples in the current folder.

See `tests.py` for more details.

