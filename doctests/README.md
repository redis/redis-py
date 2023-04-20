# Command examples for redis.io

## How to add an example

Create regular python file in the current folder with meaningful name. It makes sense prefix example files with 
command category (e.g. string, set, list, hash, etc) to make navigation in the folder easier. Files ending in *.py* 
are automatically run by the test suite.

### Special markup

See https://github.com/redis-stack/redis-stack-website#readme for more details.

## How to test examples

Just include necessary assertions in the example file and run
```bash
sh doctests/run_examples.sh
```
to test all examples in the current folder.