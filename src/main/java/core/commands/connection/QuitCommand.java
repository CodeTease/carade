package core.commands.connection;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class QuitCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
    }
}
