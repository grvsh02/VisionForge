-- Rolling-window circuit breaker for one model (in Redis, so it survives a worker restart).
-- KEYS[1] = brk:{model} (hash), KEYS[2] = brk:{model}:win (list of '1'/'0' outcomes)
-- ARGV: op(allow|record|release|trip), now_s, success('1'|'0'), window, threshold, open_s, max_probes
-- allow   -> {decision(allow|probe|deny), wait_s, degraded_since, state}
-- record  -> {state}
-- release -> {state}   (return an unused half-open probe permit)
-- trip    -> {state}   (open immediately, e.g. on 401/403: a configuration problem)
local key, win = KEYS[1], KEYS[2]
local op = ARGV[1]
local now = tonumber(ARGV[2])
local success = ARGV[3] == '1'
local window = tonumber(ARGV[4])
local threshold = tonumber(ARGV[5])
local open_s = tonumber(ARGV[6])
local max_probes = tonumber(ARGV[7])

local h = redis.call('HMGET', key, 'state', 'opened_at', 'probes', 'successes', 'degraded_since')
local state = h[1] or 'closed'
local opened_at = tonumber(h[2]) or 0
local probes = tonumber(h[3]) or 0
local successes = tonumber(h[4]) or 0
local degraded = h[5] or ''

local function trip()
  redis.call('HSET', key, 'state', 'open', 'opened_at', tostring(now), 'probes', '0', 'successes', '0')
  if degraded == '' then redis.call('HSET', key, 'degraded_since', tostring(now)) end
  redis.call('HINCRBY', key, 'trips', 1)
end

if op == 'allow' then
  if state == 'open' then
    if now - opened_at < open_s then
      return {'deny', tostring(open_s - (now - opened_at)), degraded, 'open'}
    end
    state = 'half_open'
    probes = 0
    redis.call('HSET', key, 'state', 'half_open', 'opened_at', tostring(now), 'probes', '0', 'successes', '0')
  end
  if state == 'half_open' then
    -- Probes lost to crashed workers must not wedge the breaker half-open forever.
    if probes >= max_probes and now - opened_at >= open_s then
      probes = 0
      redis.call('HSET', key, 'opened_at', tostring(now), 'probes', '0')
    end
    if probes < max_probes then
      redis.call('HINCRBY', key, 'probes', 1)
      return {'probe', '0', degraded, 'half_open'}
    end
    return {'deny', '1', degraded, 'half_open'}
  end
  return {'allow', '0', '', 'closed'}
end

if op == 'trip' then
  trip()
  return {'open'}
end

if op == 'release' then
  if state == 'half_open' and probes > 0 then
    redis.call('HSET', key, 'probes', tostring(probes - 1))
  end
  return {state}
end

-- op == 'record'
if state == 'half_open' then
  if success then
    successes = successes + 1
    if successes >= max_probes then
      redis.call('HSET', key, 'state', 'closed', 'probes', '0', 'successes', '0')
      redis.call('HDEL', key, 'degraded_since')
      redis.call('DEL', win)
      return {'closed'}
    end
    redis.call('HSET', key, 'successes', tostring(successes))
    return {'half_open'}
  end
  trip()
  return {'open'}
end
if state == 'open' then
  return {'open'}  -- late responses from before the trip are ignored
end

redis.call('LPUSH', win, success and '1' or '0')
redis.call('LTRIM', win, 0, window - 1)
if not success then
  local outcomes = redis.call('LRANGE', win, 0, -1)
  if #outcomes >= window then
    local fails = 0
    for _, v in ipairs(outcomes) do if v == '0' then fails = fails + 1 end end
    if fails / #outcomes >= threshold then
      trip()
      return {'open'}
    end
  end
end
return {'closed'}
