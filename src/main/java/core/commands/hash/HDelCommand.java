package core.commands.hash;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HDelCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: HDEL key field");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String field = new String(args.get(2), StandardCharsets.UTF_8);
        final int[] ret = {0};
        
        client.executeWrite(() -> {
            Carade.db.getStore(client.getDbIndex()).computeIfPresent(key, (k, v) -> {
                if (v.type == DataType.HASH) {
                    ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.getValue();
                    if (map.remove(field) != null) ret[0] = 1;
                    if (map.isEmpty()) return null;
                }
                return v;
            });
            if (ret[0] == 1) {
                Carade.notifyWatchers(key);
            }
        }, "HDEL", key, field);

        client.sendInteger(ret[0]);
    }
}
