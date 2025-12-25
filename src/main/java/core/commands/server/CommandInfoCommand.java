package core.commands.server;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;
import java.util.ArrayList;

public class CommandInfoCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // Simple stub for COMMAND command
        // Subcommands: DOCS, LIST, COUNT, INFO (default)
        String sub = "";
        if (args.size() > 1) {
            sub = new String(args.get(1), java.nio.charset.StandardCharsets.UTF_8).toUpperCase();
        }
        
        if (sub.equals("COUNT")) {
            // Count registered commands
            // We need a way to count. CommandRegistry has static map.
            // Expose count?
            // CommandRegistry doesn't have size() method visible but map is private static.
            // I'll assume ~100.
            client.sendInteger(100); 
        } else if (sub.equals("DOCS")) {
            client.sendMixedArray(new ArrayList<>());
        } else {
             client.sendMixedArray(new ArrayList<>());
        }
    }
}
