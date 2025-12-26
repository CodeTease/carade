package core.commands.transaction;

import core.commands.Command;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class WatchCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (client.isInTransaction()) {
            client.sendError("ERR WATCH inside MULTI is not allowed");
        } else {
            if (args.size() < 2) {
                client.sendError("usage: WATCH key [key ...]");
            } else {
                for (int i = 1; i < args.size(); i++) {
                    String key = new String(args.get(i), StandardCharsets.UTF_8);
                    client.addWatch(key);
                }
                client.sendSimpleString("OK");
            }
        }
    }
}
