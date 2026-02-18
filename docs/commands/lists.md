# Lists

Lists are simply lists of strings, sorted by insertion order. You can add elements to a Redis List on the head (on the left) or on the tail (on the right).

## Supported Commands

Carade supports standard Redis List commands.

*   **LPUSH key value**: Prepend one or multiple values to a list.
*   **RPUSH key value**: Append one or multiple values to a list.
*   **LPOP key**: Remove and get the first element in a list.
*   **RPOP key**: Remove and get the last element in a list.
*   **LRANGE key start stop**: Get a range of elements from a list.
*   **BLPOP key timeout**: Remove and get the first element in a list, or block until one is available.

See the full list in [Compatibility Matrix](compatibility.md).
