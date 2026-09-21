local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local duration = tonumber(ARGV[1])
local member = ARGV[2]
local complete = true
for i = 1, #KEYS do
    redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', now)
    if redis.call('ZSCORE', KEYS[i], member) == false then
        complete = false
    end
end
for i = 1, #KEYS do
    redis.call('ZREM', KEYS[i], member)
    if redis.call('ZCARD', KEYS[i]) > 0 then
        redis.call('PEXPIRE', KEYS[i], 2 * duration)
    end
end
if complete then return 1 end
return 0
