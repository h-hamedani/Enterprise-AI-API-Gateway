local redis_time = redis.call("TIME")
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local bucket_count = #KEYS
local states = {}
local allowed = 1
local maximum_retry_ms = 0

for index = 1, bucket_count do
    local capacity = tonumber(ARGV[(index - 1) * 2 + 1])
    local window_ms = tonumber(ARGV[(index - 1) * 2 + 2])
    local capacity_units = capacity * window_ms
    local available = redis.call("HGET", KEYS[index], "available_units")
    local last_refill_ms = redis.call("HGET", KEYS[index], "last_refill_ms")

    if available == false or last_refill_ms == false then
        available = capacity_units
        last_refill_ms = now_ms
    else
        available = tonumber(available)
        last_refill_ms = tonumber(last_refill_ms)
        local elapsed_ms = now_ms - last_refill_ms
        if elapsed_ms < 0 then
            elapsed_ms = 0
        elseif elapsed_ms > window_ms then
            elapsed_ms = window_ms
        end
        local refill_units = elapsed_ms * capacity
        local capacity_deficit = capacity_units - available
        if refill_units >= capacity_deficit then
            available = capacity_units
        else
            available = available + refill_units
        end
    end

    if available < window_ms then
        allowed = 0
        local deficit_units = window_ms - available
        local retry_ms = math.floor((deficit_units + capacity - 1) / capacity)
        if retry_ms > maximum_retry_ms then
            maximum_retry_ms = retry_ms
        end
    end

    states[index] = {available, window_ms}
end

for index = 1, bucket_count do
    local available = states[index][1]
    local window_ms = states[index][2]
    if allowed == 1 then
        available = available - window_ms
    end
    redis.call(
        "HSET",
        KEYS[index],
        "available_units", string.format("%.0f", available),
        "last_refill_ms", string.format("%.0f", now_ms)
    )
    redis.call("PEXPIRE", KEYS[index], 2 * window_ms)
end

return {allowed, maximum_retry_ms}
