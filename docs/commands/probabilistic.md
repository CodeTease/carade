# Probabilistic Data Structures

Carade includes probabilistic data structures which are extremely memory efficient at the cost of a small, controlled error rate.

## HyperLogLog

HyperLogLog is used to estimate the cardinality of a set (number of unique elements).

*   **PFADD key element...**: Add elements to the HyperLogLog.
*   **PFCOUNT key...**: Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).
*   **PFMERGE destkey sourcekey...**: Merge multiple HyperLogLog values into a single unique value.

## Bloom Filter

A Bloom filter is a probabilistic data structure that is used to test whether an element is a member of a set.

*   **BF.ADD key item**: Add an item to the Bloom Filter.
*   **BF.EXISTS key item**: Check if an item exists in the Bloom Filter.
*   **BF.MADD key item...**: Add multiple items.
*   **BF.MEXISTS key item...**: Check for multiple items.

## T-Digest

T-Digest is a data structure for accurate on-line accumulation of rank-based statistics such as quantiles and trimmed means.

*   **TD.ADD key value...**: Add one or more samples to a sketch.
*   **TD.QUANTILE key quantile...**: Get the value at a specific quantile (0.0 to 1.0).
*   **TD.CDF key value...**: Get the cumulative distribution function (CDF) for a given value.
*   **TD.INFO key**: Get information about the sketch.

## Complex Analytics Pipeline Example

You can combine HyperLogLog, Sorted Sets, and Lua scripting to build a powerful analytics pipeline. For example, calculating time-series aggregations of unique visitors per hour and rolling them up into a daily leaderboard.

### Lua Script Implementation

By using a Lua script, we ensure the multi-step transformation happens atomically on the server side, reducing network round-trips.

```lua
-- script.lua
-- KEYS[1]: The daily leaderboard Sorted Set
-- KEYS[2]: The hourly HyperLogLog for the current hour
-- ARGV[1]: The user ID
-- ARGV[2]: The hour identifier (e.g., '2023102714')

local leaderboard_key = KEYS[1]
local hll_hour_key = KEYS[2]
local user_id = ARGV[1]
local hour_id = ARGV[2]

-- 1. Add the user to the hourly HyperLogLog
redis.call('PFADD', hll_hour_key, user_id)

-- 2. Get the estimated unique count for the hour
local unique_count = redis.call('PFCOUNT', hll_hour_key)

-- 3. Update the daily leaderboard Sorted Set with this hour's unique count
redis.call('ZADD', leaderboard_key, unique_count, hour_id)

return unique_count
```

### Execution Example (Python redis-py)

```python
import redis

r = redis.Redis(host='localhost', port=63790, password='teasertopsecret')

lua_script = """
local leaderboard_key = KEYS[1]
local hll_hour_key = KEYS[2]
local user_id = ARGV[1]
local hour_id = ARGV[2]
redis.call('PFADD', hll_hour_key, user_id)
local unique_count = redis.call('PFCOUNT', hll_hour_key)
redis.call('ZADD', leaderboard_key, unique_count, hour_id)
return unique_count
"""

# Register the script once
pipeline_script = r.register_script(lua_script)

day_leaderboard = "analytics:daily:20231027"
hour_key = "analytics:hll:2023102714"
user_id = "user_9942"

# Execute the atomic transformation
current_uniques = pipeline_script(
    keys=[day_leaderboard, hour_key], 
    args=[user_id, "hour_14"]
)
print(f"Current unique visitors for hour 14: {current_uniques}")
```

### Limitations & Considerations

When designing complex pipelines combining these structures, keep the following in mind:

1.  **Approximation Errors:** HyperLogLog provides an *estimate* of cardinality. While the standard error is typically less than 1%, it is not guaranteed to be exact. Using it to drive logic that requires 100% precision will lead to bugs.
2.  **Memory Trade-offs:** While HLLs use extremely little memory (max 12KB) compared to storing all elements in a standard Set, creating thousands of fine-grained HLL keys (e.g., per-minute per-item) can still consume significant memory. Always plan your key lifecycle (`EXPIRE`).
3.  **Script Atomicity:** Lua scripts block the Carade event loop. A script performing `PFMERGE` on thousands of large HyperLogLogs could stall the server, delaying all other client requests. Ensure your scripts execute rapidly.
4.  **Error Handling:** If an error occurs inside the Lua script (e.g., calling `ZADD` on a key that already exists as a String), the script aborts, but changes made by earlier commands (like `PFADD`) **are not rolled back**. Carade scripts are atomic in isolation but do not provide full transaction abort/rollback semantics.

See the full list in [Compatibility Matrix](compatibility.md).
