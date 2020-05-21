--[[
This script is expected to atomically check the existence of a key and if and only if that
exists and is a reader value will the reader being requested actually be removed from the
values reader list. If the current value is owned by some writer then a 1 is returned. If
the current reader that is requesting removal is not in the current reader list a 2 is
returned. Otherwise 3 or 4 is returned to indicate success conditions.  If some kind of other
error happens -1 or -2 is returned.

Takes 3 inputs, the key to modify and the reader 'token' to use and the key expiration/ttl.

The counterpart of this code is in `RedisSharedLock` (this code by itself
is not the full locking solution, but only together with that code is it a full locking
solution).
--]]
local key = KEYS[1]
local reader = ARGV[1]
local ttl = ARGV[2]
if redis.call("EXISTS", key) == 0 then
    -- Key no longer found anymore, eck, get out.
    return 0
else
    local data = cmsgpack.unpack(redis.call("GET", key))
    if data["exclusive"] == true then
        -- Currently locked by some writer.
        return 1
    else
        local curr_readers = data["readers"]
        if curr_readers[reader] ~= true then
            -- Requesting reader isn't an active reader...
            return 2
        else
            curr_readers[reader] = nil
            data["readers"] = curr_readers
            if next(curr_readers) == nil then
                -- No readers left (we were the last); delete the key and let others recreate it.
                local st = redis.call("DEL", key)
                if st ~= 1 then
                    -- Key should not be able to disappear (because we are checking for it
                    -- existing at the start of this script and scripts are executed atomically
                    -- but just incase it does).
                    return -1
                else
                    return 3
                end
            else
                -- See https://github.com/antirez/redis/issues/3231 for why we look at the ok field.
                local r = redis.call("SET", key, cmsgpack.pack(data), "ex", ttl)
                local r_v = r["ok"]
                if r_v == "OK" then
                    return 4
                else
                    return -2
                end
            end
        end
    end
end
