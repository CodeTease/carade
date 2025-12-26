package core.commands.connection;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class ClientIdCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        client.sendInteger(System.identityHashCode(client));
    }
}
