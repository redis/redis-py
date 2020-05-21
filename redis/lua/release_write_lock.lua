--[[
This script is expected to atomically check the existence of a key and if and only if that
key exists and it is a writer value with the requesting writer token then it will be removed
and 1 will be returned; otherwise 0 will be returned for non-existent keys. 2 will be returned
for a non-exclusive key existing at the given value and 3 will be returned to indicate that the
current writer key is not held by the requesting token/writer.  If some kind of other
error happens -1 is returned.

Takes 2 inputs, the key to modify and the writer 'token' to use.

The counterpart of this code is in `RedisSharedLock` (this code by itself
is not the full locking solution, but only together with that code is it a full locking
solution).
--]]
local key = KEYS[1]
local writer = ARGV[1]
if redis.call("EXISTS", key) == 0 then
    -- Key no longer found anymore, eck, get out.
    return 0
else
    local data = cmsgpack.unpack(redis.call("GET", key))
    if data["exclusive"] == false then
        return 2
    elseif data["writer"] ~= writer then
        return 3
    else
        local st = redis.call("DEL", key)
        -- Key should not be able to disappear (because we are checking for it
        -- existing at the start of this script and scripts are executed atomically
        -- but just incase it does).
        if st ~= 1 then
            return -1
        else
            return 1
        end
    end
end
