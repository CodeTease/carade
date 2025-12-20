package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PttlCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: PTTL key");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);
        
        if (entry == null) {
            client.sendResponse(Resp.integer(-2), "(integer) -2");
        } else if (entry.getExpireAt() == -1) {
            client.sendResponse(Resp.integer(-1), "(integer) -1");
        } else {
            long ttl = entry.getExpireAt() - System.currentTimeMillis();
            if (ttl < 0) {
                Carade.db.remove(client.dbIndex, key);
                client.sendResponse(Resp.integer(-2), "(integer) -2");
            } else {
                client.sendResponse(Resp.integer(ttl), "(integer) " + ttl);
            }
        }
    }
}
