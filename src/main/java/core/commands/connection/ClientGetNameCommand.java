package core.commands.connection;

import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ClientGetNameCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        String name = client.getClientName();
        if (name == null) {
            client.sendResponse(Resp.bulkString((byte[])null), "(nil)");
        } else {
            client.sendResponse(Resp.bulkString(name.getBytes(StandardCharsets.UTF_8)), name);
        }
    }
}
