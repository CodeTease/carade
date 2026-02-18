# JSON (Carade Extension)

Carade provides native JSON support, allowing you to store, update, and retrieve JSON values associated with keys. It is similar to the RedisJSON module.

## Supported Commands

Carade supports basic JSON commands.

*   **JSON.SET key path value**: Set the JSON value at `key` with the given `path` (use `$` for root).
*   **JSON.GET key [path ...]**: Get the JSON value at `key`.
*   **JSON.DEL key [path]**: Delete a value at `path`.
*   **JSON.TYPE key [path]**: Report the type of JSON value at `path`.

Example:

```bash
JSON.SET user:1 $ '{"name":"John", "age":30}'
JSON.GET user:1
```

See the full list in [Compatibility Matrix](compatibility.md).
