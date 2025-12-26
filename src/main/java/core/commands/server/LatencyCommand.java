package core.commands.server;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.Collections;
import java.util.List;

public class LatencyCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // Stub implementation
        client.sendArray(Collections.emptyList());
    }
}
