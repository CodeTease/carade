package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

public class ZDiffStoreCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: ZDIFFSTORE destkey numkeys key [key ...]");
            return;
        }

        String destKey = new String(args.get(1), StandardCharsets.UTF_8);
        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }
        
        if (args.size() != 3 + numKeys) {
            client.sendError("ERR syntax error");
            return;
        }

        String firstKey = new String(args.get(3), StandardCharsets.UTF_8);
        
        // Log args for AOF
        Object[] logArgs = new Object[args.size()-1];
        for(int i=1; i<args.size(); i++) logArgs[i-1] = new String(args.get(i), StandardCharsets.UTF_8);
        
        client.executeWrite(() -> {
            ValueEntry v = Carade.db.get(client.getDbIndex(), firstKey);
            if (v == null || v.type != DataType.ZSET) {
                // If first key empty, result empty.
                Carade.db.remove(client.getDbIndex(), destKey);
                client.sendInteger(0);
                return;
            }
            
            CaradeZSet firstZSet = (CaradeZSet) v.getValue();
            Set<String> exclusion = new HashSet<>();
            
            for (int i = 1; i < numKeys; i++) {
                 String key = new String(args.get(3 + i), StandardCharsets.UTF_8);
                 ValueEntry ev = Carade.db.get(client.getDbIndex(), key);
                 if (ev != null && ev.type == DataType.ZSET) {
                     CaradeZSet z = (CaradeZSet) ev.getValue();
                     exclusion.addAll(z.scores.keySet());
                 }
            }
            
            CaradeZSet newZSet = new CaradeZSet();
            for (ZNode node : firstZSet.sorted) {
                if (!exclusion.contains(node.member)) {
                    newZSet.add(node.score, node.member);
                }
            }
            
            if (newZSet.scores.isEmpty()) {
                Carade.db.remove(client.getDbIndex(), destKey);
                client.sendInteger(0);
            } else {
                Carade.db.put(client.getDbIndex(), destKey, new ValueEntry(newZSet, DataType.ZSET, -1));
                client.sendInteger(newZSet.scores.size());
            }
            
        }, "ZDIFFSTORE", logArgs);
    }
}
