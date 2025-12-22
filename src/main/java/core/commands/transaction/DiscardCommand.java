package core.commands.transaction;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class DiscardCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (!client.isInTransaction()) {
            client.sendError("ERR DISCARD without MULTI");
        } else {
            client.setInTransaction(false);
            client.clearTransactionQueue();
            client.unwatchAll();
            client.setTransactionDirty(false);
            client.sendSimpleString("OK");
        }
    }
}
