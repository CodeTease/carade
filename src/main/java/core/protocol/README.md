# Protocol Module

This module handles the Redis Serialization Protocol (RESP) parsing and encoding using Netty.

## NettyRespDecoder

`core.protocol.netty.NettyRespDecoder` is a `ByteToMessageDecoder` that handles the parsing of incoming byte streams from clients. It ensures that fragmented packets are correctly reassembled into complete Redis commands.

### Mechanism
The decoder uses a state machine to parse the stream:
*   **READ_TYPE**: Reads the first byte to determine the type (e.g., `*` for Array, inline commands).
*   **READ_LINE_LENGTH**: Reads the length of the array or the line content.
*   **READ_BULK_LENGTH**: Reads the length of a specific bulk string argument (prefixed with `$`).
*   **READ_BULK_CONTENT**: Reads the actual raw bytes of the argument based on the length.

### Output
The decoder produces a `List<byte[]>` (wrapped as an Object) representing a single command and its arguments. This object is passed to the `CommandHandler` for execution.

## Resp Class

`core.protocol.Resp` provides static utility methods for:

*   **Encoding**: Generating RESP-compliant byte arrays for responses.
    *   `simpleString(String)`: `+OK\r\n`
    *   `error(String)`: `-ERR message\r\n`
    *   `integer(long)`: `:100\r\n`
    *   `bulkString(byte[])`: `$3\r\nval\r\n`
    *   `array(List)`: `*2\r\n$3\r\nkey\r\n$3\r\nval\r\n`
*   **Parsing**: A legacy `parse(InputStream)` method is available for blocking IO contexts (e.g., loading AOF or initial replication syncs not using Netty).
