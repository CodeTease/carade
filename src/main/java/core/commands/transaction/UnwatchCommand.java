package core.commands.transaction;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class UnwatchCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        client.unwatchAll();
        client.setTransactionDirty(false);
        client.sendSimpleString("OK");
    }
}
