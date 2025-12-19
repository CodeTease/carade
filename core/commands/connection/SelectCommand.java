package core.commands.connection;

import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SelectCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: SELECT index");
            return;
        }
        
        try {
            int index = Integer.parseInt(new String(args.get(1), StandardCharsets.UTF_8));
            if (index == 0) {
                client.sendResponse(Resp.simpleString("OK"), "OK");
            } else {
                client.sendError("ERR DB index is out of range");
            }
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
