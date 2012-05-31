local changed_ts_key = KEYS[1] .. '.decay'
local last_ms = tonumber(redis.call('get', changed_ts_key))
local now_ts = redis.call('time')
local now_ms = (1000 * now_ts[1]) + now_ts[2]/1000
local dt_ms = 0
if last_ms == nil then
   dt_ms = 0
else
   dt_ms = now_ms - last_ms
end

local last_updated_val = tonumber(redis.call('get', KEYS[1]))
if last_updated_val == nil then
   last_updated_val = 0      
end

local current_val = last_updated_val - math.floor( dt_ms / %(decay_ms)d )
local new_val = tonumber(ARGV[1])
local ttl = %(default_expiration_ms)d
if current_val >= 0 then
   ttl = (new_val * %(decay_ms)d) + %(default_expiration_ms)d
   new_val = new_val + current_val
end

redis.call('psetex', changed_ts_key, ttl, now_ms) 
redis.call('psetex', KEYS[1], ttl, new_val)
return {last_updated_val, dt_ms, current_val, new_val}
