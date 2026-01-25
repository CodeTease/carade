package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

public class ZDiffCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: ZDIFF numkeys key [key ...]");
            return;
        }

        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(args.get(1), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }

        if (args.size() != 2 + numKeys) {
        }

        boolean withScores = false;
        if (args.size() > 2 + numKeys) {
            String lastArg = new String(args.get(args.size() - 1), StandardCharsets.UTF_8).toUpperCase();
            if (lastArg.equals("WITHSCORES")) {
                withScores = true;
            } else {
                client.sendError("ERR syntax error");
                return;
            }
        }

        String firstKey = new String(args.get(2), StandardCharsets.UTF_8);
        ValueEntry v = Carade.db.get(client.getDbIndex(), firstKey);
        
        // ZDIFF: Elements in first set that are NOT in others.
        // If first key missing, result empty.
        
        if (v == null || v.type != DataType.ZSET) {
            client.sendArray(new ArrayList<>());
            return;
        }
        
        CaradeZSet firstZSet = (CaradeZSet) v.getValue();
        
        // We can copy the first set logic, then remove elements present in others.
        // Since we need to output ordered result (by score), we can iterate firstZSet.sorted
        // and check if member exists in others.
        
        List<byte[]> result = new ArrayList<>();
        
        // Build exclusion set from other keys
        // We only care about member presence for diff.
        Set<String> exclusion = new HashSet<>();
        
        for (int i = 1; i < numKeys; i++) {
             String key = new String(args.get(2 + i), StandardCharsets.UTF_8);
             ValueEntry ev = Carade.db.get(client.getDbIndex(), key);
             if (ev != null && ev.type == DataType.ZSET) {
                 CaradeZSet z = (CaradeZSet) ev.getValue();
                 exclusion.addAll(z.scores.keySet());
             }
        }
        
        for (ZNode node : firstZSet.sorted) {
            if (!exclusion.contains(node.member)) {
                result.add(node.member.getBytes(StandardCharsets.UTF_8));
                if (withScores) {
                    result.add(String.valueOf(node.score).getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        
        client.sendArray(result);
    }
}
