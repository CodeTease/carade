# Hashes

Hashes are maps between string fields and string values, so they are the perfect data type to represent objects (e.g., A User with a number of fields like name, surname, age, and so forth).

## Supported Commands

Carade supports standard Redis Hash commands.

*   **HSET key field value**: Set the string value of a hash field.
*   **HGET key field**: Get the value of a hash field.
*   **HMSET key field value ...**: Set multiple hash fields to multiple values.
*   **HGETALL key**: Get all the fields and values in a hash.
*   **HDEL key field**: Delete one or more hash fields.
*   **HINCRBY key field increment**: Increment the integer value of a hash field by the given number.

See the full list in [Compatibility Matrix](compatibility.md).
