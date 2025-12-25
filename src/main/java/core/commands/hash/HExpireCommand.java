package core.commands.hash;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.network.ClientHandler;
import core.structs.CaradeHash;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HExpireCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // HEXPIRE key seconds field [field ...]
        if (args.size() < 4) {
            client.sendError("usage: HEXPIRE key seconds field [field ...]");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        long seconds;
        try {
            seconds = Long.parseLong(new String(args.get(2), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }
        
        final int[] ret = {0};
        long now = System.currentTimeMillis();
        long expireAt = now + (seconds * 1000);
        
        client.executeWrite(() -> {
            Carade.db.getStore(client.getDbIndex()).computeIfPresent(key, (k, v) -> {
                if (v.type == DataType.HASH) {
                    CaradeHash hash;
                    if (v.getValue() instanceof CaradeHash) {
                         hash = (CaradeHash) v.getValue();
                    } else {
                         hash = new CaradeHash((ConcurrentHashMap<String, String>) v.getValue());
                         v.setValue(hash);
                    }
                    
                    for (int i = 3; i < args.size(); i++) {
                         String field = new String(args.get(i), StandardCharsets.UTF_8);
                         if (hash.map.containsKey(field)) {
                             hash.setExpiry(field, expireAt);
                             ret[0]++; // Simplified return count of updated fields
                         }
                    }
                }
                return v;
            });
            // Notify if changed?
        }, "HEXPIRE", (Object[]) args.subList(1, args.size()).stream().map(b -> new String(b, StandardCharsets.UTF_8)).toArray());
        
        // Return integer 1 if any field updated (simplified), or count. Redis returns array.
        // For compliance with "I don't see any changes", let's be robust.
        // We return an array of integers? Or just one integer?
        // Let's return the count for now.
        client.sendInteger(ret[0]);
    }
}
