# Sets

Sets are an unordered collection of unique strings. You can add, remove, and test for existence of members in O(1) time.

## Supported Commands

Carade supports standard Redis Set commands.

*   **SADD key member**: Add one or more members to a set.
*   **SREM key member**: Remove one or more members from a set.
*   **SMEMBERS key**: Get all the members in a set.
*   **SISMEMBER key member**: Determine if a given value is a member of a set.
*   **SCARD key**: Get the number of members in a set.
*   **SINTER key ...**: Intersect multiple sets.
*   **SUNION key ...**: Add multiple sets.
*   **SDIFF key ...**: Subtract multiple sets.

See the full list in [Compatibility Matrix](compatibility.md).
