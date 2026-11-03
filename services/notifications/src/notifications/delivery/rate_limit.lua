-- One account, one hash. Validate all persisted fields before the first write:
-- Redis Lua errors do not roll back earlier redis.call writes.
local key = KEYS[1]
local mode, fingerprint, selected = ARGV[1], ARGV[2], ARGV[3]
local pause = tonumber(ARGV[4])
local names = {'total', 'transactional', 'service', 'bulk'}
local rates, capacities = {}, {}
local function finite(n)
    return n and n == n and n ~= math.huge and n ~= -math.huge
end
local function number_string(n)
    return string.format('%.17g', n)
end
local function denied(reason)
    return {0, number_string(pause), reason}
end
for i = 1, 4 do
    rates[i], capacities[i] = tonumber(ARGV[4 + i * 2 - 1]), tonumber(ARGV[4 + i * 2])
    if not finite(rates[i]) or rates[i] <= 0 or not finite(capacities[i]) or
        capacities[i] < 1 or capacities[i] ~= math.floor(capacities[i]) then
        return denied('invalid_config')
    end
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
if mode == 'initialize' then
    if redis.call('EXISTS', key) == 1 then return {0, '0', 'already_exists'} end
    local cooldown = tonumber(ARGV[13])
    if not finite(cooldown) or cooldown <= 0 or not finite(now + cooldown) then
        return denied('invalid_cooldown')
    end
    local ready = now + cooldown
    local fields = {'schema', '1', 'fingerprint', fingerprint, 'ready_at', number_string(ready)}
    for i = 1, 4 do
        table.insert(fields, names[i] .. '_tokens')
        table.insert(fields, '0')
        table.insert(fields, names[i] .. '_updated_at')
        table.insert(fields, number_string(ready))
    end
    redis.call('HSET', key, unpack(fields))
    return {1, '0', 'initialized'}
end
local kind = redis.call('TYPE', key).ok
if kind == 'none' then return denied('missing_state') end
if kind ~= 'hash' or redis.call('PTTL', key) ~= -1 then return denied('invalid_state') end
local state = redis.call('HGETALL', key)
if #state ~= 22 then return denied('invalid_state') end
local values = {}
for i = 1, #state, 2 do values[state[i]] = state[i + 1] end
if values.schema ~= '1' then return denied('invalid_state') end
if values.fingerprint ~= fingerprint then return denied('config_mismatch') end
local ready = tonumber(values.ready_at)
if not finite(ready) or ready < 0 then return denied('invalid_state') end
local tokens, updated = {}, {}
for i = 1, 4 do
    tokens[i], updated[i] = tonumber(values[names[i] .. '_tokens']),
        tonumber(values[names[i] .. '_updated_at'])
    if not finite(tokens[i]) or tokens[i] < 0 or tokens[i] > capacities[i] or
        not finite(updated[i]) or updated[i] < ready then
        return denied('invalid_state')
    end
    -- ready_at may be in the future during the deliberate initialization cooldown.
    -- A later timestamp signals clock rollback or damage: never refill or rewind it.
    if updated[i] > math.max(now, ready) then return denied('clock_backwards') end
end
if mode == 'status' then return {1, '0', 'healthy'} end
if mode ~= 'acquire' then return denied('invalid_operation') end
local index = nil
for i = 2, 4 do if names[i] == selected then index = i end end
if not index then return denied('invalid_class') end
local chosen = {1, index}
local delay = 0
for _, i in ipairs(chosen) do
    tokens[i] = math.min(capacities[i], tokens[i] + math.max(0, now - updated[i]) * rates[i])
    if tokens[i] < 1 then
        delay = math.max(delay, math.max(0, updated[i] - now) + (1 - tokens[i]) / rates[i])
    end
end
if not finite(delay) then return denied('invalid_state') end
if now < ready then delay = math.max(delay, ready - now) end
if delay > 0 then return {0, number_string(delay), 'rate_limited'} end
-- All validation and calculations completed. Write both balances together.
redis.call('HSET', key,
    'total_tokens', number_string(tokens[1] - 1),
    'total_updated_at', number_string(math.max(now, updated[1])),
    selected .. '_tokens', number_string(tokens[index] - 1),
    selected .. '_updated_at', number_string(math.max(now, updated[index])))
return {1, '0', 'allowed'}
