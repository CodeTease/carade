package core.commands.hash;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HmgetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("wrong number of arguments for 'hmget' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);

        List<byte[]> results = new ArrayList<>();
        
        if (entry == null) {
            // Key not found -> return nils for all requested fields
            for (int i = 2; i < args.size(); i++) {
                results.add(null);
            }
        } else if (entry.type != DataType.HASH) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        } else {
            ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) entry.getValue();
            for (int i = 2; i < args.size(); i++) {
                String field = new String(args.get(i), StandardCharsets.UTF_8);
                String val = map.get(field);
                results.add(val != null ? val.getBytes(StandardCharsets.UTF_8) : null);
            }
        }

        client.sendArray(results);
    }
}
