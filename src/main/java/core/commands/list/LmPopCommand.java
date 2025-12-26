package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LmPopCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
        if (args.size() < 3) {
            client.sendError("usage: LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]");
            return;
        }
        
        try {
            int numKeys = Integer.parseInt(new String(args.get(1), StandardCharsets.UTF_8));
            if (numKeys <= 0) {
                 client.sendError("ERR numkeys must be positive");
                 return;
            }
            if (args.size() < 2 + numKeys + 1) { // numkeys arg + keys + direction
                 client.sendError("ERR syntax error");
                 return;
            }
            
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < numKeys; i++) {
                keys.add(new String(args.get(2 + i), StandardCharsets.UTF_8));
            }
            
            String direction = new String(args.get(2 + numKeys), StandardCharsets.UTF_8).toUpperCase();
            if (!direction.equals("LEFT") && !direction.equals("RIGHT")) {
                client.sendError("ERR syntax error");
                return;
            }
            boolean isLeft = direction.equals("LEFT");
            
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
                ValueEntry v = Carade.db.get(client.dbIndex, k);
                if (v != null && v.type == DataType.LIST) {
                     ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
                     if (!list.isEmpty()) {
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
            final boolean finalIsLeft = isLeft;
            final List<String> popped = new ArrayList<>();
            
            client.executeWrite(() -> {
                 ValueEntry v = Carade.db.get(client.dbIndex, finalKey);
                 if (v != null && v.type == DataType.LIST) {
                     ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
                     for (int i=0; i<finalCount; i++) {
                         String val = finalIsLeft ? list.pollFirst() : list.pollLast();
                         if (val != null) popped.add(val);
                         else break;
                     }
                     
                     if (list.isEmpty()) {
                         Carade.db.remove(client.dbIndex, finalKey);
                     }
                     Carade.notifyWatchers(finalKey);
                 }
            }, "LMPOP", targetKey, String.valueOf(count), direction);
            
            if (popped.isEmpty()) {
                client.sendNull();
            } else {
                List<byte[]> resp = new ArrayList<>();
                resp.add(finalKey.getBytes(StandardCharsets.UTF_8));
                List<byte[]> elements = new ArrayList<>();
                for (String s : popped) elements.add(s.getBytes(StandardCharsets.UTF_8));
                resp.add(Resp.array(elements));
                
                client.sendResponse(Resp.array(resp), null);
            }
            
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
