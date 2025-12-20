package core.commands.replication;

import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.util.List;

public class ReplconfCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // Just ack
        client.sendResponse(Resp.simpleString("OK"), "OK");
    }
}
