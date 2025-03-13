# EXAMPLE: pipe_trans_tutorial
# HIDE_START
"""
Code samples for vector database quickstart pages:
    https://redis.io/docs/latest/develop/get-started/vector-database/
"""
# HIDE_END
import redis

# STEP_START basic_pipe
r = redis.Redis(decode_responses=True)
# REMOVE_START
for i in range(5):
    r.delete(f"seat:{i}")

r.delete("shellpath")
# REMOVE_END

pipe = r.pipeline()

for i in range(5):
    pipe.set(f"seat:{i}", f"#{i}")

set_5_result = pipe.execute()
print(set_5_result)  # >>> [True, True, True, True, True]

pipe = r.pipeline()

# "Chain" pipeline commands together.
get_3_result = pipe.get("seat:0").get("seat:3").get("seat:4").execute()
print(get_3_result)  # >>> ['#0', '#3', '#4']
# STEP_END
# REMOVE_START
assert set_5_result == [True, True, True, True, True]
assert get_3_result == ['#0', '#3', '#4']
# REMOVE_END

# STEP_START trans_watch
r.set("shellpath", "/usr/syscmds/")

with r.pipeline() as pipe:
    # Repeat until successful.
    while True:
        try:
            # Watch the key we are about to change.
            pipe.watch("shellpath")

            # The pipeline executes commands directly (instead of
            # buffering them) from immediately after the `watch()`
            # call until we begin the transaction.
            current_path = pipe.get("shellpath")
            new_path = current_path + ":/usr/mycmds/"

            # Start the transaction, which will enable buffering
            # again for the remaining commands.
            pipe.multi()

            pipe.set("shellpath", new_path)

            pipe.execute()

            # The transaction succeeded, so break out of the loop.
            break
        except redis.WatchError:
            # The transaction failed, so continue with the next attempt.
            continue

get_path_result = r.get("shellpath")
print(get_path_result)  # >>> '/usr/syscmds/:/usr/mycmds/'
# STEP_END
# REMOVE_START
assert get_path_result == '/usr/syscmds/:/usr/mycmds/'
r.delete("shellpath")
# REMOVE_END

# STEP_START watch_conv_method
r.set("shellpath", "/usr/syscmds/")


def watched_sequence(pipe):
    current_path = pipe.get("shellpath")
    new_path = current_path + ":/usr/mycmds/"

    pipe.multi()

    pipe.set("shellpath", new_path)


trans_result = r.transaction(watched_sequence, "shellpath")
print(trans_result)  # True

get_path_result = r.get("shellpath")
print(get_path_result)  # >>> '/usr/syscmds/:/usr/mycmds/'
# REMOVE_START
assert trans_result
assert get_path_result == '/usr/syscmds/:/usr/mycmds/'
# REMOVE_END
# STEP_END
