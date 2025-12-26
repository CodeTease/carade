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
import java.util.concurrent.ConcurrentHashMap;

public class HKeysCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("wrong number of arguments for 'hkeys' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);

        if (entry == null || entry.type != DataType.HASH) {
            client.sendArray(Collections.emptyList());
        } else {
            ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) entry.getValue();
            List<byte[]> keys = new ArrayList<>();
            for (String k : map.keySet()) {
                keys.add(k.getBytes(StandardCharsets.UTF_8));
            }
            client.sendArray(keys);
        }
    }
}
