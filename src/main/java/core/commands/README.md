# Commands Module

This module contains the implementations of all Redis commands supported by Carade, organized by category (string, list, set, etc.).

## CommandRegistry

The `core.commands.CommandRegistry` is the central repository for command definitions. It maintains a static map:
`Map<String, CommandContainer>`

Each `CommandContainer` holds:
1.  **Command Instance**: The singleton instance of the class implementing `Command`.
2.  **Metadata**: Defined by `CommandMetadata` (arity, flags, key positions).

## How to Add a New Command

To implement a new command (e.g., `MYCMD key value`), follow these steps:

### 1. Implement the Command Interface
Create a new class in the appropriate package (e.g., `core.commands.string`).

```java
package core.commands.string;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class MyCmdCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // Note: args includes the command name at index 0.
        // e.g., for "MYCMD key val", args.size() is 3.
        
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'mycmd' command");
            return;
        }
        
        byte[] key = args.get(1);
        byte[] val = args.get(2);
        
        // Execute logic...
        client.sendSimpleString("OK");
    }
}
```

### 2. Register the Command
Open `core.commands.CommandRegistry.java` and add a registration line in the `static` block.

```java
// Arity: -3 (means >= 3 args: MYCMD key value). 
// Flags: "write" (affects data), "denyoom" (reject if out of memory).
// FirstKey: 1, LastKey: 1, Step: 1 (The key is at index 1).
register("MYCMD", new MyCmdCommand(), new CommandMetadata(-3, Set.of("write", "denyoom"), 1, 1, 1));
```

### 3. Metadata Parameters
*   **Arity**: Positive integer `N` means exact match. Negative integer `-N` means at least `N` arguments.
*   **Flags**: Set of strings like `readonly`, `write`, `denyoom`, `fast`.
*   **Key Specs**: `firstKey`, `lastKey`, `step` define which arguments are keys (used for tracking and key extraction).
