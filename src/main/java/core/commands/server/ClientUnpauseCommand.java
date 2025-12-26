package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class ClientUnpauseCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        Carade.pauseEndTime = 0;
        client.sendSimpleString("OK");
    }
}
