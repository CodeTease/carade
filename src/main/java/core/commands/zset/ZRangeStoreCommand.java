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
import java.util.Collections;

public class ZRangeStoreCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 5) {
            client.sendError("usage: ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]");
            return;
        }

        String dst = new String(args.get(1), StandardCharsets.UTF_8);
        String src = new String(args.get(2), StandardCharsets.UTF_8);
        
        client.executeWrite(() -> {
            ValueEntry v = Carade.db.get(client.getDbIndex(), src);
            if (v == null || v.type != DataType.ZSET) {
                Carade.db.remove(client.getDbIndex(), dst);
                client.sendInteger(0);
                return;
            }
            
            // Logic similar to ZRANGE but store
            
            // Argument parsing
            String minStr = new String(args.get(3), StandardCharsets.UTF_8);
            String maxStr = new String(args.get(4), StandardCharsets.UTF_8);
            
            boolean byScore = false;
            boolean byLex = false;
            boolean rev = false;
            int offset = 0;
            int count = Integer.MAX_VALUE;
            
            int idx = 5;
            while (idx < args.size()) {
                String arg = new String(args.get(idx), StandardCharsets.UTF_8).toUpperCase();
                if (arg.equals("BYSCORE")) byScore = true;
                else if (arg.equals("BYLEX")) byLex = true;
                else if (arg.equals("REV")) rev = true;
                else if (arg.equals("LIMIT")) {
                    if (idx + 2 >= args.size()) {
                         throw new RuntimeException("ERR syntax error");
                    }
                    offset = Integer.parseInt(new String(args.get(idx+1), StandardCharsets.UTF_8));
                    count = Integer.parseInt(new String(args.get(idx+2), StandardCharsets.UTF_8));
                    idx += 2;
                }
                idx++;
            }
            
            CaradeZSet zset = (CaradeZSet) v.getValue();
            List<ZNode> result = new ArrayList<>();
            
            if (byScore) {
                 // ZRANGEBYSCORE logic
                 // Reuse ZRangeByScore logic? Hard without public method.
                 // Basic impl:
                 double min, max;
                 boolean minExc = minStr.startsWith("(");
                 boolean maxExc = maxStr.startsWith("(");
                 try {
                     min = minExc ? Double.parseDouble(minStr.substring(1)) : Double.parseDouble(minStr);
                     max = maxExc ? Double.parseDouble(maxStr.substring(1)) : Double.parseDouble(maxStr);
                 } catch (Exception e) {
                     throw new RuntimeException("ERR min or max is not a float");
                 }
                 
                 // Iterate sorted
                 for (ZNode node : zset.sorted) {
                     boolean gteMin = minExc ? node.score > min : node.score >= min;
                     boolean lteMax = maxExc ? node.score < max : node.score <= max;
                     if (gteMin && lteMax) {
                         result.add(node);
                     }
                 }
                 
                 if (rev) Collections.reverse(result);
                 
            } else if (byLex) {
                 result.addAll(zset.sorted); 
            } else {
                 // Index based
                 int start = Integer.parseInt(minStr);
                 int end = Integer.parseInt(maxStr);
                 int size = zset.size();
                 
                 if (start < 0) start = size + start;
                 if (end < 0) end = size + end;
                 if (start < 0) start = 0;
                 if (end >= size) end = size - 1;
                 if (start > end || start >= size) {
                     result = new ArrayList<>();
                 } else {
                     // Need O(N) iteration as SkipListSet doesn't support random access
                     int current = 0;
                     for (ZNode node : zset.sorted) {
                         if (current >= start && current <= end) {
                             result.add(node);
                         }
                         current++;
                         if (current > end) break;
                     }
                 }
                 
                 if (rev) Collections.reverse(result);
            }

            // Apply LIMIT
            if (offset > 0 || count != Integer.MAX_VALUE) {
                 if (offset >= result.size()) {
                     result.clear();
                 } else {
                     int to = Math.min(result.size(), offset + count);
                     result = new ArrayList<>(result.subList(offset, to));
                 }
            }
            
            // Store
            if (result.isEmpty()) {
                Carade.db.remove(client.getDbIndex(), dst);
                client.sendInteger(0);
            } else {
                CaradeZSet newZ = new CaradeZSet();
                for (ZNode node : result) newZ.add(node.score, node.member);
                Carade.db.put(client.getDbIndex(), dst, new ValueEntry(newZ, DataType.ZSET, -1));
                client.sendInteger(newZ.size());
            }

        }, "ZRANGESTORE", args.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toArray());
    }
}
