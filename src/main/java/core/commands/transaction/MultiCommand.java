package core.commands.transaction;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class MultiCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (client.isInTransaction()) {
            client.sendError("ERR MULTI calls can not be nested");
        } else {
            client.setInTransaction(true);
            client.clearTransactionQueue();
            client.sendSimpleString("OK");
        }
    }
}
