--[[
This script is expected to atomically check the existence of a key and if and only if that
key does not exist then the key is set to be a writer value (with the token of the writer
that called as the owner); zero is used to as a return value to indicate failure and
one is used otherwise. If some kind of other error happens -1 is returned.

Takes 3 inputs, the key to modify and the writer 'token' to use and the key expiration/ttl.

The counterpart of this code is in `RedisSharedLock` (this code by itself
is not the full locking solution, but only together with that code is it a full locking
solution).
--]]
local key = KEYS[1]
local writer = ARGV[1]
local ttl = ARGV[2]
if redis.call("EXISTS", key) == 1 then
    -- Key already being used by someone else (reader or writer), get out.
    return 0
else
    local data = {}
    data["exclusive"] = true
    data["writer"] = writer
    -- See https://github.com/antirez/redis/issues/3231 for why we look at the ok field.
    local r = redis.call("SET", key, cmsgpack.pack(data), "ex", ttl)
    local r_v = r["ok"]
    if r_v == "OK" then
        return 1
    else
        return -1
    end
end
