package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class DbSizeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        client.sendInteger(Carade.db.size(client.getDbIndex()));
    }
}
