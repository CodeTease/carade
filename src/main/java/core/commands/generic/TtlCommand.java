package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TtlCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: TTL key");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        if (entry == null) client.sendInteger(-2);
        else if (entry.expireAt == -1) client.sendInteger(-1);
        else {
            long ttl = (entry.expireAt - System.currentTimeMillis()) / 1000;
            if (ttl < 0) {
                Carade.db.remove(client.getDbIndex(), key);
                client.sendInteger(-2);
            } else {
                client.sendInteger(ttl);
            }
        }
    }
}
