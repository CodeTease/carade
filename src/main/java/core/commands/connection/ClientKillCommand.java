package core.commands.connection;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ClientKillCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'client kill' command");
            return;
        }
        
        // CLIENT KILL [ip:port] [ID client-id] ...
        // Simplified: CLIENT KILL ID <id> or CLIENT KILL ADDR <addr> (implicitly second arg)
        
        String filterType = new String(args.get(2), StandardCharsets.UTF_8).toUpperCase();
        int killed = 0;
        
        if (filterType.equals("ID") && args.size() >= 4) {
             try {
                 long id = Long.parseLong(new String(args.get(3), StandardCharsets.UTF_8));
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
             for (ClientHandler c : Carade.connectedClients) {
                 if (c.getRemoteAddress().contains(addr)) {
                     c.close();
                     killed++;
                 }
             }
             client.sendInteger(killed);
        }
    }
}
