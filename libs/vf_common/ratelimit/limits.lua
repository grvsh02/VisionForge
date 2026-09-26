-- Token bucket and concurrency slots for one model, kept in Redis so they survive a worker restart.
--
-- bucket: KEYS[1] = bucket:<stage>   ARGV: 'take', now_s, rate, max_wait_s
--         -> {granted(0|1), wait_s}. A grant may carry a wait (a reserved future slot),
--            which spaces callers out instead of letting them all retry at once.
--         The bucket holds at most one token, so calls are always spaced 1/rate apart: an
--         idle or restarted worker can't fire a burst at a provider running at its limit.
--         KEYS[1] = bucket:<stage>   ARGV: 'freeze', until_s   (429: nothing until Retry-After)
-- slots:  KEYS[1] = slots:<stage> (sorted set holder -> expiry)
--         ARGV: 'acquire', now_s, holder, limit, ttl_s -> 1 | 0
--         ARGV: 'refresh', now_s, holder, ttl_s        (holder still working)
--         ARGV: 'release', holder
-- Expired slot holders (calls of a crashed worker) are dropped, so their capacity comes back.
-- retry:  KEYS[1] = current window, KEYS[2] = previous window (win:<stage>:<id> hashes)
--         ARGV: 'retry', ratio, minimum, ttl_s -> 1 (retry reserved) | 0 (budget spent)
--         Check and reserve in one step, so concurrent retry tasks can't all slip past the budget.
local key, op = KEYS[1], ARGV[1]

if op == 'take' then
  local now, rate, max_wait = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
  local h = redis.call('HMGET', key, 'tokens', 'ts', 'frozen_until')
  local tokens, ts, frozen = tonumber(h[1]) or 1, tonumber(h[2]) or now, tonumber(h[3]) or 0
  if frozen > now then return {0, tostring(frozen - now)} end
  if now > ts then tokens = math.min(1, tokens + (now - ts) * rate); ts = now end
  if tokens < 1 - rate * max_wait then return {0, tostring((1 - rate * max_wait - tokens) / rate)} end
  tokens = tokens - 1
  redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(ts))
  redis.call('EXPIRE', key, 3600)
  if tokens < 0 then return {1, tostring(-tokens / rate)} end
  return {1, '0'}
elseif op == 'freeze' then
  local until_s = ARGV[2]
  redis.call('HSET', key, 'tokens', '0', 'ts', until_s, 'frozen_until', until_s)
  return 1
elseif op == 'acquire' then
  local now, holder, limit, ttl = tonumber(ARGV[2]), ARGV[3], tonumber(ARGV[4]), tonumber(ARGV[5])
  redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
  if redis.call('ZCARD', key) >= limit then return 0 end
  redis.call('ZADD', key, now + ttl, holder)
  return 1
elseif op == 'refresh' then
  redis.call('ZADD', key, 'XX', tonumber(ARGV[2]) + tonumber(ARGV[4]), ARGV[3])
  return 1
elseif op == 'release' then
  redis.call('ZREM', key, ARGV[2])
  return 1
elseif op == 'retry' then
  local function count(k, field) return tonumber(redis.call('HGET', k, field)) or 0 end
  local first = count(KEYS[1], 'first') + count(KEYS[2], 'first')
  local retries = count(KEYS[1], 'retries') + count(KEYS[2], 'retries')
  if retries >= math.max(tonumber(ARGV[3]), tonumber(ARGV[2]) * first) then return 0 end
  redis.call('HINCRBY', KEYS[1], 'retries', 1)
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
  return 1
end
return redis.error_reply('unknown op ' .. tostring(op))
