--[[
This script is expected to atomically check the existence of a key and if it
does not exist, then set the value to be a reader (with a single entry for the current
readers and then return 1 (success). If it already exists then it must ensure that the
lock is not a reader lock (if it is then 0 is used to return that status code) and otherwise
the current reader list is updated and the value is adjusted and set and then
return 1 (success). If some kind of other error happens -1 is returned.

Takes 3 inputs, the key to modify and the reader 'token' to use and the key expiration/ttl.

The counterpart of this code is in `RedisSharedLock` (this code by itself
is not the full locking solution, but only together with that code is it a full locking
solution).
--]]
local key = KEYS[1]
local reader = ARGV[1]
local ttl = ARGV[2]
if redis.call("EXISTS", key) == 0 then
    local data = {}
    local readers = {}
    readers[reader] = true
    data["exclusive"] = false
    data["readers"] = readers
    -- See https://github.com/antirez/redis/issues/3231 for why we look at the ok field.
    local r = redis.call("SET", key, cmsgpack.pack(data), "ex", ttl)
    local r_v = r["ok"]
    if r_v == "OK" then
        return 1
    else
        return -1
    end
else
    local data = cmsgpack.unpack(redis.call("GET", key))
    if data["exclusive"] == true then
        return 0
    else
        data["readers"][reader] = true
        -- See https://github.com/antirez/redis/issues/3231 for why we look at the ok field.
        local r = redis.call("SET", key, cmsgpack.pack(data), "ex", ttl)
        local r_v = r["ok"]
        if r_v == "OK" then
            return 1
        else
            return -1
        end
    end
end
