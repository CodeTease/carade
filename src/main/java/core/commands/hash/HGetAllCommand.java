package core.commands.hash;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HGetAllCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: HGETALL key");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        
        if (entry == null || entry.type != DataType.HASH) {
            client.sendArray(Collections.emptyList());
        } else {
            ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) entry.getValue();
            List<byte[]> flat = new ArrayList<>();
            for (Map.Entry<String, String> e : map.entrySet()) {
                flat.add(e.getKey().getBytes(StandardCharsets.UTF_8));
                flat.add(e.getValue().getBytes(StandardCharsets.UTF_8));
            }
            client.sendArray(flat);
        }
    }
}
