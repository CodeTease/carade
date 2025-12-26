package core.commands.server;

import core.commands.Command;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MemoryCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'memory' command");
            return;
        }
        
        String sub = new String(args.get(1), StandardCharsets.UTF_8).toUpperCase();
        
        if (sub.equals("USAGE")) {
            new MemoryUsageCommand().execute(client, args);
        } else if (sub.equals("STATS")) {
            new MemoryStatsCommand().execute(client, args);
        } else {
            client.sendError("ERR unknown subcommand for 'memory'. Try USAGE, STATS.");
        }
    }
}
