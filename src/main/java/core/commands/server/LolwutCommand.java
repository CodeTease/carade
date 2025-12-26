package core.commands.server;

import core.commands.Command;
import core.network.ClientHandler;
import java.util.List;

public class LolwutCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        String msg = 
            "Carade v0.2.0\n" +
            "Running on Java 21\n" +
            "  .   .\n" +
            "   \\_/\n" +
            "  (o o)\n" +
            "  (   )\n" +
            "   \" \"";
        client.sendBulkString(msg);
    }
}
