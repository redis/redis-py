# Command examples for redis.io

## How to add example

Create regular python file in the current folder with meaningful name. It makes sense prefix example files with 
command category (e.g. string, set, list, hash, etc) to make navigation in the folder easier.

**Keep in mind that file names should be consistent across all clients.**

### Special markup

#### `HIDE_START` and `HIDE_END`
Should be used to hide imports, connection creation and other bootstrapping code that is not important
for understanding a command example.

Example:

```python
# HIDE_START
import redis

r = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True
)
# HIDE_END
```

#### `REMOVE_START` and `REMOVE_END`
Should be used to **remove** code from the resulting code snippet published on redis.io.
This markup is useful to remove assertions, and other testing blocks.

```python
res = r.set("foo", "bar")
print(res)
# >>> True
# REMOVE_START
assert res
# REMOVE_END

res = r.get("foo")
print(res)
# >>> "bar"
# REMOVE_START
assert res == "bar"
# REMOVE_END
```

## How to test example

Just include necessary assertions in the example file and run
```bash
pytest -v tests.py
```
to test all examples in the current folder.

See `tests.py` for more details.

