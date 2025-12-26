package core.commands.hash;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HSetNxCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: HSETNX key field value");
            return;
        }

        Carade.performEvictionIfNeeded();
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String field = new String(args.get(2), StandardCharsets.UTF_8);
        String val = new String(args.get(3), StandardCharsets.UTF_8);
        final int[] ret = {0};
        
        client.executeWrite(() -> {
            Carade.db.getStore(client.getDbIndex()).compute(key, (k, v) -> {
                if (v == null) {
                    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
                    map.put(field, val);
                    ret[0] = 1;
                    return new ValueEntry(map, DataType.HASH, -1);
                } else if (v.type != DataType.HASH) {
                    throw new RuntimeException("WRONGTYPE");
                } else {
                    ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.getValue();
                    if (map.putIfAbsent(field, val) == null) {
                        ret[0] = 1;
                    }
                    v.touch();
                    return v;
                }
            });
            if (ret[0] == 1) Carade.notifyWatchers(key);
        }, "HSETNX", key, field, val);
        
        client.sendInteger(ret[0]);
    }
}
