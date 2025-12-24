package core.commands.connection;

import core.Carade;
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
                // Using hashCode as simple ID since we don't have unique long IDs yet
                // Or maybe the activeConnections count is not unique enough. 
                // Using System.identityHashCode or just the object hash.
                client.sendInteger(System.identityHashCode(client));
                break;
            case "LIST":
                StringBuilder sb = new StringBuilder();
                for (ClientHandler c : Carade.connectedClients) {
                    sb.append("id=").append(System.identityHashCode(c))
                      .append(" addr=").append(c.getRemoteAddress())
                      .append(" name=").append(c.getClientName() == null ? "" : c.getClientName())
                      .append(" db=").append(c.getDbIndex())
                      .append("\n");
                }
                client.sendBulkString(sb.toString());
                break;
            case "KILL":
                if (args.size() < 3) {
                    client.sendError("ERR wrong number of arguments for 'client kill' command");
                    return;
                }
                // Only implementing basic KILL ID <id> or KILL ADDR <addr> could be complex parsing
                // For simplicity, let's assume argument is ID or ADDR.
                // Redis syntax: CLIENT KILL [ip:port] [ID client-id] ...
                // We'll support: CLIENT KILL ID <id>
                String filterType = new String(args.get(2), StandardCharsets.UTF_8).toUpperCase();
                
                if (filterType.equals("ID") && args.size() >= 4) {
                     try {
                         long id = Long.parseLong(new String(args.get(3), StandardCharsets.UTF_8));
                         int killed = 0;
                         for (ClientHandler c : Carade.connectedClients) {
                             if (System.identityHashCode(c) == id) {
                                 c.close();
                                 killed++;
                             }
                         }
                         client.sendInteger(killed);
                     } catch (NumberFormatException e) {
                         client.sendError("ERR value is not an integer or out of range");
                     }
                } else {
                     // Try to treat arg 2 as address
                     String addr = new String(args.get(2), StandardCharsets.UTF_8);
                     int killed = 0;
                     for (ClientHandler c : Carade.connectedClients) {
                         if (c.getRemoteAddress().contains(addr)) {
                             c.close();
                             killed++;
                         }
                     }
                     client.sendInteger(killed);
                }
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
