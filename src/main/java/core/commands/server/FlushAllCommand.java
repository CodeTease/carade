package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class FlushAllCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        client.executeWrite(() -> {
            for (String k : Carade.watchers.keySet()) {
                Carade.notifyWatchers(k);
            }
            Carade.db.clearAll(); 
        }, "FLUSHALL");
        client.sendSimpleString("OK");
    }
}
