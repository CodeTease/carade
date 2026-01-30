package core.commands.set;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class SPopCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("wrong number of arguments for 'spop' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        int count = 1;
        boolean hasCount = false;
        if (args.size() > 2) {
            try {
                count = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
                hasCount = true;
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not an integer or out of range");
                return;
            }
        }

        final int finalCount = count;
        final boolean finalHasCount = hasCount;
        final List<String> popped = new java.util.ArrayList<>();

        client.executeWrite(() -> {
            Carade.db.getStore(client.dbIndex).computeIfPresent(key, (k, v) -> {
                if (v.type != DataType.SET) return v;

                Set<String> set = (Set<String>) v.getValue();
                if (set.isEmpty()) return null; // Should have been removed already if empty

                int size = set.size();
                int todo = finalCount;
                if (todo > size) todo = size;
                
                Object[] arr = set.toArray();
                
                for (int i = 0; i < todo; i++) {
                    // Pick random from remaining
                    int idx = ThreadLocalRandom.current().nextInt(arr.length - i);
                    
                    Object picked = arr[idx];
                    popped.add((String) picked);
                    
                    // Move last available to this spot so we don't pick it again
                    arr[idx] = arr[arr.length - 1 - i];
                }

                // Remove from actual set
                for (String s : popped) {
                    set.remove(s);
                }

                if (set.isEmpty()) return null;
                return v;
            });
            if (!popped.isEmpty()) Carade.notifyWatchers(key);
        }, "SPOP", key, String.valueOf(count));

        ValueEntry entry = Carade.db.get(client.dbIndex, key);
        if (entry != null && entry.type != DataType.SET) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
        } else {
             if (finalHasCount) {
                 List<byte[]> resp = new java.util.ArrayList<>();
                 for (String s : popped) resp.add(s.getBytes(StandardCharsets.UTF_8));
                 client.sendArray(resp);
             } else {
                 if (popped.isEmpty()) client.sendNull();
                 else client.sendBulkString(popped.get(0));
             }
        }
    }
}
