package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TypeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: TYPE key");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        client.sendSimpleString(entry == null ? "none" : entry.type.name().toLowerCase());
    }
}
