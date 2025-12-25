package core.commands.set;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SInterCardCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: SINTERCARD numkeys key [key ...] [LIMIT limit]");
            return;
        }

        try {
            int numKeys = Integer.parseInt(new String(args.get(1), StandardCharsets.UTF_8));
            if (numKeys <= 0) {
                 client.sendError("ERR numkeys must be positive");
                 return;
            }
            if (args.size() < 2 + numKeys) {
                 client.sendError("ERR syntax error");
                 return;
            }

            int limit = 0; // 0 means unlimited
            if (args.size() > 2 + numKeys) {
                String nextArg = new String(args.get(2 + numKeys), StandardCharsets.UTF_8).toUpperCase();
                if (nextArg.equals("LIMIT")) {
                     if (args.size() <= 2 + numKeys + 1) {
                         client.sendError("ERR syntax error");
                         return;
                     }
                     limit = Integer.parseInt(new String(args.get(2 + numKeys + 1), StandardCharsets.UTF_8));
                     if (limit < 0) {
                         client.sendError("ERR limit must be positive");
                         return;
                     }
                } else {
                     client.sendError("ERR syntax error");
                     return;
                }
            }

            String firstKey = new String(args.get(2), StandardCharsets.UTF_8);
            ValueEntry entry = Carade.db.get(client.getDbIndex(), firstKey);
            
            if (entry == null || entry.type != DataType.SET) {
                 client.sendInteger(0);
                 return;
            }
            
            Set<String> result = new HashSet<>((Set<String>) entry.getValue());
            for (int i = 1; i < numKeys; i++) {
                 if (result.isEmpty()) break;
                 if (limit > 0 && result.size() <= limit) {
                     // Optimization: if intermediate result is small, we still intersect?
                     // Actually, intersection only shrinks. So if we are already below limit, we just continue.
                     // The limit applies to FINAL cardinality.
                 }
                 
                 ValueEntry e = Carade.db.get(client.getDbIndex(), new String(args.get(2 + i), StandardCharsets.UTF_8));
                 if (e == null || e.type != DataType.SET) {
                     result.clear();
                     break;
                 }
                 result.retainAll((Set<String>) e.getValue());
            }
            
            int size = result.size();
            if (limit > 0 && size > limit) size = limit;
            
            client.sendInteger(size);
        } catch (NumberFormatException e) {
             client.sendError("ERR value is not an integer or out of range");
        }
    }
}
