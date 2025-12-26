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
import java.util.concurrent.ConcurrentLinkedQueue;

public class BlmPopCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
        if (args.size() < 4) {
            client.sendError("usage: BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]");
            return;
        }

        try {
            double timeout = Double.parseDouble(new String(args.get(1), StandardCharsets.UTF_8));
            int numKeys = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
            
            if (numKeys <= 0) {
                 client.sendError("ERR numkeys must be positive");
                 return;
            }
            if (args.size() < 3 + numKeys + 1) { 
                 client.sendError("ERR syntax error");
                 return;
            }
            
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < numKeys; i++) {
                keys.add(new String(args.get(3 + i), StandardCharsets.UTF_8));
            }
            
            String direction = new String(args.get(3 + numKeys), StandardCharsets.UTF_8).toUpperCase();
            boolean isLeft = direction.equals("LEFT");
            if (!isLeft && !direction.equals("RIGHT")) {
                client.sendError("ERR syntax error");
                return;
            }

            int count = 1;
            if (args.size() > 3 + numKeys + 1) {
                String nextArg = new String(args.get(3 + numKeys + 1), StandardCharsets.UTF_8).toUpperCase();
                if (nextArg.equals("COUNT")) {
                     if (args.size() <= 3 + numKeys + 2) {
                         client.sendError("ERR syntax error");
                         return;
                     }
                     count = Integer.parseInt(new String(args.get(3 + numKeys + 2), StandardCharsets.UTF_8));
                }
            }

            // 1. Try to serve immediately
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
            
            if (targetKey != null) {
                // Same logic as LMPOP
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
                         if (list.isEmpty()) Carade.db.remove(client.dbIndex, finalKey);
                         Carade.notifyWatchers(finalKey);
                     }
                }, "BLMPOP", targetKey, String.valueOf(count), direction);
                
                List<byte[]> resp = new ArrayList<>();
                resp.add(finalKey.getBytes(StandardCharsets.UTF_8));
                List<byte[]> elements = new ArrayList<>();
                for (String s : popped) elements.add(s.getBytes(StandardCharsets.UTF_8));
                resp.add(Resp.array(elements));
                client.sendResponse(Resp.array(resp), null);
                return;
            }
            
            // 2. Block
            Carade.BlockingRequest bReq = new Carade.BlockingRequest(client, isLeft, null, client.getDbIndex());
            for (String k : keys) {
                Carade.blockingRegistry.computeIfAbsent(k, x -> new ConcurrentLinkedQueue<>()).add(bReq);
            }
            
            bReq.future.whenComplete((result, ex) -> {
                 if (ex != null || result == null) {
                     client.sendNull();
                 } else {
                     // Result is [key, val]
                     // BLMPOP expects [key, [val]]
                     List<byte[]> resp = new ArrayList<>();
                     resp.add(result.get(0)); // key
                     
                     List<byte[]> inner = new ArrayList<>();
                     inner.add(result.get(1)); // val
                     
                     resp.add(Resp.array(inner));
                     client.sendResponse(Resp.array(resp), null);
                 }
            });
            
            if (timeout > 0) {
                client.scheduleTimeout(bReq, (long)(timeout * 1000));
            }

        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
