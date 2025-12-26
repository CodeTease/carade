package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class FlushDbCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        client.executeWrite(() -> {
            Carade.db.clear(client.getDbIndex());
        }, "FLUSHDB");
        client.sendSimpleString("OK");
    }
}
