package core.commands.connection;

import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class EchoCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 2) {
            client.sendError("usage: ECHO message");
            return;
        }
        byte[] msg = args.get(1);
        client.sendResponse(Resp.bulkString(msg), new String(msg, StandardCharsets.UTF_8));
    }
}
