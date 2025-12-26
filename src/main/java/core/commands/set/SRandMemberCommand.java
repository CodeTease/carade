package core.commands.set;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class SRandMemberCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("wrong number of arguments for 'srandmember' command");
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

        ValueEntry entry = Carade.db.get(client.dbIndex, key);
        if (entry == null) {
            if (hasCount) client.sendArray(Collections.emptyList());
            else client.sendNull();
            return;
        }

        if (entry.type != DataType.SET) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }

        Set<String> set = (Set<String>) entry.getValue();
        if (set.isEmpty()) {
             if (hasCount) client.sendArray(Collections.emptyList());
             else client.sendNull();
             return;
        }

        Object[] arr = set.toArray();
        List<byte[]> result = new ArrayList<>();
        
        if (!hasCount) {
            // Just one random member
            int idx = ThreadLocalRandom.current().nextInt(arr.length);
            client.sendBulkString((String) arr[idx]);
            return;
        }

        // Behavior depends on sign of count
        // Positive count: distinct elements, up to size.
        // Negative count: allows duplicates, return |count| elements.
        
        int absCount = Math.abs(count);
        
        if (count > 0) {
            if (absCount >= arr.length) {
                // Return all
                for (Object o : arr) result.add(((String)o).getBytes(StandardCharsets.UTF_8));
            } else {
                // Pick distinct
                // Shuffle logic on the array copy or similar
                // Since we have array copy, we can just pick random unique indices
                // Or Fisher-Yates partial shuffle
                for (int i = 0; i < absCount; i++) {
                    int idx = i + ThreadLocalRandom.current().nextInt(arr.length - i);
                    Object temp = arr[i];
                    arr[i] = arr[idx];
                    arr[idx] = temp;
                    result.add(((String)arr[i]).getBytes(StandardCharsets.UTF_8));
                }
            }
        } else {
            // Allow duplicates
            for (int i = 0; i < absCount; i++) {
                int idx = ThreadLocalRandom.current().nextInt(arr.length);
                result.add(((String)arr[idx]).getBytes(StandardCharsets.UTF_8));
            }
        }
        
        client.sendArray(result);
    }
}
