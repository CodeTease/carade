# Scripting Module

This module implements Lua scripting support (EVAL, EVALSHA) using the `Luaj` library.

## ScriptManager

`core.scripting.ScriptManager` is a singleton that orchestrates script execution.

*   **Atomicity**: It uses a `ReentrantLock` (`executionLock`) to ensure that only one script executes at a time, preserving Redis's single-threaded atomicity guarantee.
*   **Caching**: Scripts are compiled and cached by their SHA1 digest in `scriptCache`.
*   **Safety**: It installs a debug hook to monitor instruction count, allowing the server to handle infinite loops via `SCRIPT KILL` (though strict timeouts are not fully enforceable in pure Java threads without cooperation).
*   **Context**: It injects global variables `KEYS` and `ARGV` before execution.

## LuaConverter

`core.scripting.LuaConverter` handles the type mapping between Java objects and Lua values.

### Java to Lua
*   `List` -> Lua Table (1-based index).
*   `RespParser.SimpleString` -> Lua Table `{ok: "value"}`.
*   `RespParser.RespError` -> Lua Table `{err: "message"}`.
*   `byte[]` / `String` -> Lua String.
*   `Boolean` -> Lua `1` (true) or `0` (false).
*   `null` -> Lua `false` (in boolean context).

### Lua to Java (Result)
*   Lua String -> Java `byte[]` (Bulk String).
*   Lua Table (Array) -> Java `List`.
*   Lua Table `{ok: "val"}` -> Java `String` (Simple String).
*   Lua Table `{err: "msg"}` -> Java `RespParser.RespError`.
*   Lua Boolean -> Java `Long` (1 or null/0).
*   Lua Number -> Java `Long`.

## Redis Binding

`core.scripting.RedisLuaBinding` is loaded into the Lua environment to provide the `redis.call()` and `redis.pcall()` functions, bridging Lua back to the Java `ClientHandler`.
