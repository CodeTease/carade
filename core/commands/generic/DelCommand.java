package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DelCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: DEL key");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        final int[] ret = {0};
        
        client.executeWrite(() -> {
            ValueEntry prev = Carade.db.remove(client.dbIndex, key);
            if (prev != null) {
                Carade.notifyWatchers(key);
                ret[0] = 1;
            }
        }, "DEL", key);
        
        client.sendResponse(Resp.integer(ret[0]), "(integer) " + ret[0]);
    }
}
