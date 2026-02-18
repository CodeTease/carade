# Sorted Sets (ZSets)

Sorted Sets are similar to Sets but every member is associated with a score (floating point number). This allows retrieving a range of elements (e.g. give me the top 10, or the bottom 10).

## Supported Commands

Carade supports standard Redis Sorted Set commands.

*   **ZADD key score member**: Add one or more members to a sorted set, or update its score.
*   **ZRANGE key start stop [WITHSCORES]**: Return a range of members in a sorted set, by index.
*   **ZREVRANGE key start stop [WITHSCORES]**: Return a range of members in a sorted set, by index, with scores ordered from high to low.
*   **ZRANK key member**: Determine the index of a member in a sorted set.
*   **ZSCORE key member**: Get the score associated with the given member in a sorted set.
*   **ZREM key member**: Remove one or more members from a sorted set.
*   **ZCARD key**: Get the number of members in a sorted set.
*   **ZCOUNT key min max**: Count the members in a sorted set with scores within the given values.

See the full list in [Compatibility Matrix](compatibility.md).
