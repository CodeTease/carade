package core.commands.connection;

import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ClientSetNameCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendResponse(Resp.error("usage: CLIENT SETNAME connection-name"), "(error) usage: CLIENT SETNAME connection-name");
            return;
        }
        String name = new String(args.get(2), StandardCharsets.UTF_8);
        client.setClientName(name);
        client.sendResponse(Resp.simpleString("OK"), "OK");
    }
}
