# Network Layer

Carade uses **Netty**, a high-performance asynchronous event-driven network application framework, to handle the **RESP (Redis Serialization Protocol)**.

## Netty Pipeline

The Netty pipeline is configured as follows:

1.  **NettyRespDecoder (Inbound):**
    *   Reads raw bytes from the client.
    *   Parses the RESP format into a `List<byte[]>`, where each byte array represents a command argument.
    *   Handles fragmentation and packet reassembly.

2.  **NettyRespEncoder (Outbound):**
    *   Converts Java objects (String, Integer, List, etc.) into the RESP byte stream to be sent back to the client.
    *   Example: `Integer(5)` -> `:5\r\n` (Integer reply).

3.  **ClientHandler (Inbound):**
    *   Contains the core logic for executing commands.
    *   Maintains client-specific state (Authentication, Selected DB, Blocking Queues).
    *   Routes the command to the `CommandRegistry` for execution.

## RESP Protocol Implementation

Carade implements the RESP protocol strictly.

### Request Format
Clients send commands as an Array of Bulk Strings.

```
*3\r\n
$3\r\n
SET\r\n
$5\r\n
mykey\r\n
$7\r\n
myvalue\r\n
```

### Response Formats
Carade supports standard RESP types:

*   **Simple Strings:** `+OK\r\n`
*   **Errors:** `-ERR unknown command\r\n`
*   **Integers:** `:1000\r\n`
*   **Bulk Strings:** `$6\r\nfoobar\r\n`
*   **Arrays:** `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`

## Connection Management

*   **Active Connections:** Tracked via `Carade.connectedClients`.
*   **Cleanup:** When a client disconnects, `ClientHandler.channelInactive()` ensures resources (blocking requests, watchers) are cleaned up.
*   **Blocking Operations:** Handled via `Carade.blockingRegistry`. When a client calls `BLPOP`, the request is stored in a queue. When data arrives (via `LPUSH`), the blocking client is notified and the response is sent asynchronously.
