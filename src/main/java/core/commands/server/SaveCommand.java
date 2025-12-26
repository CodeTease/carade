package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class SaveCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        try {
            Carade.saveData();
            client.sendSimpleString("OK");
        } catch (RuntimeException e) {
            client.sendError("ERR " + e.getMessage());
        }
    }
}
