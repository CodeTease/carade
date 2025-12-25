package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ZInterCardCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: ZINTERCARD numkeys key [key ...] [LIMIT limit]");
            return;
        }

        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(args.get(1), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            if (2 + i >= args.size()) {
                client.sendError("ERR syntax error");
                return;
            }
            keys.add(new String(args.get(2 + i), StandardCharsets.UTF_8));
        }
        
        long limit = 0; // 0 means unlimited
        if (args.size() > 2 + numKeys) {
             if (args.size() == 4 + numKeys && new String(args.get(2 + numKeys), StandardCharsets.UTF_8).equalsIgnoreCase("LIMIT")) {
                 try {
                     limit = Long.parseLong(new String(args.get(3 + numKeys), StandardCharsets.UTF_8));
                 } catch (NumberFormatException e) {
                     client.sendError("ERR value is not an integer or out of range");
                     return;
                 }
             } else {
                 client.sendError("ERR syntax error");
                 return;
             }
        }

        // Optimization: Find smallest set
        CaradeZSet smallest = null;
        int smallestSize = Integer.MAX_VALUE;
        
        for (String key : keys) {
            ValueEntry v = Carade.db.get(client.getDbIndex(), key);
            if (v == null || v.type != DataType.ZSET) {
                // Empty or wrong type means intersection is 0
                client.sendInteger(0);
                return;
            }
            CaradeZSet z = (CaradeZSet) v.getValue();
            if (z.scores.isEmpty()) {
                client.sendInteger(0);
                return;
            }
            if (z.scores.size() < smallestSize) {
                smallestSize = z.scores.size();
                smallest = z;
            }
        }
        
        if (smallest == null) {
            client.sendInteger(0);
            return;
        }
        
        int intersectCount = 0;
        for (String member : smallest.scores.keySet()) {
            boolean presentInAll = true;
            for (String key : keys) {
                ValueEntry v = Carade.db.get(client.getDbIndex(), key);
                CaradeZSet z = (CaradeZSet) v.getValue();
                if (!z.scores.containsKey(member)) {
                    presentInAll = false;
                    break;
                }
            }
            
            if (presentInAll) {
                intersectCount++;
                if (limit > 0 && intersectCount >= limit) break;
            }
        }
        
        client.sendInteger(intersectCount);
    }
}
