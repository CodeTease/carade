# Strings

String keys are binary safe, meaning they can contain any kind of data, for instance a JPEG image or a serialized Ruby object.
A string value can be at most 512 Megabytes.

## Supported Commands

Carade supports standard Redis String commands.

*   **SET key value**: Set the string value of a key.
*   **GET key**: Get the value of a key.
*   **INCR key**: Increment the integer value of a key by one.
*   **DECR key**: Decrement the integer value of a key by one.
*   **APPEND key value**: Append a value to a key.
*   **STRLEN key**: Get the length of the value stored in a key.

See the full list in [Compatibility Matrix](compatibility.md).
