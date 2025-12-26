package core.commands.connection;

import core.commands.Command;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ClientCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'client' command");
            return;
        }
        
        String sub = new String(args.get(1), StandardCharsets.UTF_8).toUpperCase();
        
        switch (sub) {
            case "SETNAME":
                new ClientSetNameCommand().execute(client, args);
                break;
            case "GETNAME":
                new ClientGetNameCommand().execute(client, args);
                break;
            case "ID":
                new ClientIdCommand().execute(client, args);
                break;
            case "LIST":
                new ClientListCommand().execute(client, args);
                break;
            case "KILL":
                new ClientKillCommand().execute(client, args);
                break;
            case "PAUSE":
                new core.commands.server.ClientPauseCommand().execute(client, args);
                break;
            case "UNPAUSE":
                new core.commands.server.ClientUnpauseCommand().execute(client, args);
                break;
            default:
                client.sendError("ERR unknown subcommand for 'client'");
        }
    }
}
