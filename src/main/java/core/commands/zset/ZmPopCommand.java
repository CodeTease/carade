package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ZmPopCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
        if (args.size() < 3) {
            client.sendError("usage: ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]");
            return;
        }
        
        try {
            int numKeys = Integer.parseInt(new String(args.get(1), StandardCharsets.UTF_8));
            if (numKeys <= 0) {
                 client.sendError("ERR numkeys must be positive");
                 return;
            }
            if (args.size() < 2 + numKeys + 1) { 
                 client.sendError("ERR syntax error");
                 return;
            }
            
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < numKeys; i++) {
                keys.add(new String(args.get(2 + i), StandardCharsets.UTF_8));
            }
            
            String direction = new String(args.get(2 + numKeys), StandardCharsets.UTF_8).toUpperCase();
            if (!direction.equals("MIN") && !direction.equals("MAX")) {
                client.sendError("ERR syntax error");
                return;
            }
            boolean isMin = direction.equals("MIN");
            
            int count = 1;
            if (args.size() > 2 + numKeys + 1) {
                String nextArg = new String(args.get(2 + numKeys + 1), StandardCharsets.UTF_8).toUpperCase();
                if (nextArg.equals("COUNT")) {
                     if (args.size() <= 2 + numKeys + 2) {
                         client.sendError("ERR syntax error");
                         return;
                     }
                     count = Integer.parseInt(new String(args.get(2 + numKeys + 2), StandardCharsets.UTF_8));
                     if (count <= 0) {
                          client.sendError("ERR count must be positive");
                          return;
                     }
                } else {
                     client.sendError("ERR syntax error");
                     return;
                }
            }
            
            // Logic: find first non-empty
            String targetKey = null;
            for (String k : keys) {
                ValueEntry v = Carade.db.get(client.getDbIndex(), k);
                if (v != null && v.type == DataType.ZSET) {
                     CaradeZSet zset = (CaradeZSet) v.getValue();
                     if (zset.size() > 0) {
                         targetKey = k;
                         break;
                     }
                }
            }
            
            if (targetKey == null) {
                client.sendNull();
                return;
            }
            
            // Perform Pop
            final String finalKey = targetKey;
            final int finalCount = count;
            final boolean finalIsMin = isMin;
            final List<ZNode> popped = new ArrayList<>();
            
            client.executeWrite(() -> {
                 ValueEntry v = Carade.db.get(client.getDbIndex(), finalKey);
                 if (v != null && v.type == DataType.ZSET) {
                     CaradeZSet zset = (CaradeZSet) v.getValue();
                     List<ZNode> nodes = finalIsMin ? zset.popMin(finalCount) : zset.popMax(finalCount);
                     popped.addAll(nodes);
                     
                     if (zset.size() == 0) {
                         Carade.db.remove(client.getDbIndex(), finalKey);
                     }
                     Carade.notifyWatchers(finalKey);
                 }
            }, "ZMPOP", targetKey, String.valueOf(count), direction);
            
            if (popped.isEmpty()) {
                client.sendNull();
            } else {
                List<byte[]> resp = new ArrayList<>();
                resp.add(finalKey.getBytes(StandardCharsets.UTF_8));
                List<byte[]> elements = new ArrayList<>();
                for (ZNode node : popped) {
                    List<byte[]> pair = new ArrayList<>();
                    pair.add(node.member.getBytes(StandardCharsets.UTF_8));
                    pair.add(String.valueOf(node.score).getBytes(StandardCharsets.UTF_8));
                    elements.add(Resp.array(pair));
                }
                resp.add(Resp.array(elements));
                
                client.sendResponse(Resp.array(resp), null);
            }
            
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
