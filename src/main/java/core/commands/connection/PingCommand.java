package core.commands.connection;

import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.util.List;

public class PingCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() > 1) {
            client.sendBulkString(new String(args.get(1), java.nio.charset.StandardCharsets.UTF_8));
        } else {
            client.send(client.isResp(), Resp.simpleString("PONG"), "PONG");
        }
    }
}
