package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class ClientPauseCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 2) {
            client.sendError("usage: CLIENT PAUSE timeout");
            return;
        }
        
        long timeout;
        try {
            timeout = Long.parseLong(new String(args.get(1), java.nio.charset.StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR timeout is not an integer or out of range");
            return;
        }
        
        // Carade.pauseClients(timeout);
        // We need to implement pausing mechanism.
        // Easiest is to set a volatile timestamp in Carade.
        // ClientHandler.channelRead check this timestamp.
        
        Carade.pauseEndTime = System.currentTimeMillis() + timeout;
        
        client.sendSimpleString("OK");
    }
}
