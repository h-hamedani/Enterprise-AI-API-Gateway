-- Static, tenant-slot-local circuit transition script. No untrusted Lua source.
local meta, failures = KEYS[1], KEYS[2]
local action, mode, token_id = ARGV[1], ARGV[2], ARGV[3]
local token_generation = tonumber(ARGV[4])
local candidate, probe_candidate = ARGV[5], ARGV[6]
local threshold, window = tonumber(ARGV[7]), tonumber(ARGV[8])
local open_duration, probe_duration = tonumber(ARGV[9]), tonumber(ARGV[10])
local max_safe = 9007199254740991
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local state = redis.call('HGET', meta, 'state')
if not state then
  if action ~= 'eligibility' then return {'ok', 'CLOSED', 'stale'} end
  redis.call('DEL', failures)
  redis.call('HSET', meta, 'state', 'CLOSED', 'incarnation_id', candidate, 'generation', '1')
  state = 'CLOSED'
end
local incarnation = redis.call('HGET', meta, 'incarnation_id')
local generation_string = redis.call('HGET', meta, 'generation')
local generation = tonumber(generation_string)
if not generation or generation < 1 or generation > max_safe or generation ~= math.floor(generation) then
  return {'invalid', state, 'error'}
end
if action == 'eligibility' then
  if state == 'CLOSED' then
    return {'ok', state, 'normal', incarnation, generation_string}
  end
  if state == 'OPEN' then
    local open_until = tonumber(redis.call('HGET', meta, 'open_until_ms'))
    if not open_until or now < open_until then return {'ok', state, 'denied'} end
  elseif state == 'HALF_OPEN' then
    local expires = tonumber(redis.call('HGET', meta, 'probe_expires_at_ms'))
    if not expires or now < expires then return {'ok', state, 'denied'} end
  else
    return {'invalid', state, 'error'}
  end
  if now + probe_duration > max_safe then return {'invalid', state, 'error'} end
  redis.call('HSET', meta, 'state', 'HALF_OPEN', 'probe_owner', probe_candidate,
    'probe_expires_at_ms', now + probe_duration)
  return {'ok', 'HALF_OPEN', 'probe', probe_candidate}
end
if action ~= 'success' and action ~= 'failure' then return {'invalid', state, 'error'} end
if mode == 'normal' then
  if state ~= 'CLOSED' or token_id ~= incarnation or token_generation ~= generation then
    return {'ok', state, 'stale'}
  end
  if action == 'success' then
    redis.call('DEL', failures)
    return {'ok', 'CLOSED', 'applied'}
  end
  if now + open_duration > max_safe then return {'invalid', state, 'error'} end
  redis.call('ZREMRANGEBYSCORE', failures, '-inf', now - window)
  redis.call('ZADD', failures, now, candidate)
  if redis.call('ZCOUNT', failures, '(' .. tostring(now - window), now) < threshold then
    return {'ok', 'CLOSED', 'applied'}
  end
  redis.call('HSET', meta, 'state', 'OPEN', 'open_until_ms', now + open_duration)
  redis.call('HDEL', meta, 'probe_owner', 'probe_expires_at_ms')
  redis.call('DEL', failures)
  return {'ok', 'OPEN', 'applied'}
end
if mode ~= 'probe' or state ~= 'HALF_OPEN' then return {'ok', state, 'stale'} end
local owner = redis.call('HGET', meta, 'probe_owner')
local expires = tonumber(redis.call('HGET', meta, 'probe_expires_at_ms'))
if not owner or owner ~= token_id or not expires or now >= expires then
  return {'ok', state, 'stale'}
end
if action == 'success' then
  if generation >= max_safe then return {'overflow', state, 'error'} end
  redis.call('HSET', meta, 'state', 'CLOSED', 'generation', string.format('%.0f', generation + 1))
  redis.call('HDEL', meta, 'probe_owner', 'probe_expires_at_ms', 'open_until_ms')
  redis.call('DEL', failures)
  return {'ok', 'CLOSED', 'applied'}
end
if now + open_duration > max_safe then return {'invalid', state, 'error'} end
redis.call('HSET', meta, 'state', 'OPEN', 'open_until_ms', now + open_duration)
redis.call('HDEL', meta, 'probe_owner', 'probe_expires_at_ms')
redis.call('DEL', failures)
return {'ok', 'OPEN', 'applied'}
