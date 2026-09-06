package limiter

// TokenBucketLuaScript implements an atomic lazy-refill token bucket in Redis.
// KEYS[1]: Rate limiter key (e.g., "ratelimit:ip:192.168.1.1")
// ARGV[1]: Refill rate (tokens per second)
// ARGV[2]: Max capacity (burst)
// ARGV[3]: Current timestamp in seconds (float)
// ARGV[4]: Requested tokens (usually 1.0)
//
// Returns:
// [1] allowed (1 = true, 0 = false)
// [2] remaining tokens (float string)
// [3] retry_after in seconds (float string)

const TokenLuaScript = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

-- Fetch current bucket state
local data = redis.call("HMGET", key, "tokens", "last_refill")
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

-- Initialize bucket if empty
if tokens == nil then
	tokens = capacity
	last_refill = now
else
	-- Calculate elapsed time and replenish tokens lazily
	local elapsed = math.max(0, now - last_refill)
	tokens = math.min(capacity, tokens + (elapsed * rate))
	last_refill = now
end

-- Check if enough tokens are available
local allowed = 0
local retry_after = 0

if tokens >= requested then
	allowed = 1
	tokens = tokens - requested
else
	local missing = requested - tokens
	retry_after = missing / rate
end

-- Update the bucket state in Redis
redis.call("HMSET", key, "tokens", tokens, "last_refill", last_refill)

-- Set dynamic TTL: auto-expire after time needed to fully refill
local ttl = math.ceil(capacity / rate) * 2
redis.call("EXPIRE", key, math.max(ttl, 60))

return { allowed, tostring(tokens), tostring(retry_after) }
`

// SyncOfflineDeltasLuaScript applies offline consumed tokens to the live Redis bucket.
// KEYS[1]: Rate limiter key
// ARGV[1]: Refill rate
// ARGV[2]: Max capacity
// ARGV[3]: Current timestamp
// ARGV[4]: Delta tokens to deduct
const SyncOfflineDeltasLuaScript = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local delta = tonumber(ARGV[4])

local data = redis.call("HMGET", key, "tokens", "last_refill")
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if tokens == nil then
	tokens = capacity
	last_refill = now
else
	local elapsed = math.max(0, now - last_refill)
	tokens = math.min(capacity, tokens + (elapsed * rate))
	last_refill = now
end

-- Deduct the offline consumed tokens (clamped to 0)
tokens = math.max(0, tokens - delta)
redis.call("HMSET", key, "tokens", tokens, "last_refill", last_refill)
local ttl = math.ceil(capacity / rate) * 2
redis.call("EXPIRE", key, math.max(ttl, 60))

return tokens
`
