package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import core.structs.CaradeZSet;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class ZRandMemberCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: ZRANDMEMBER key [count [WITHSCORES]]");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);
        
        if (entry == null) {
            client.send(client.isResp(), Resp.array(Collections.emptyList()), null);
            return;
        }
        
        if (entry.type != DataType.ZSET) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        
        CaradeZSet zset = (CaradeZSet) entry.getValue();
        if (zset.size() == 0) {
            client.send(client.isResp(), Resp.array(Collections.emptyList()), null);
            return;
        }

        int count = 0;
        boolean hasCount = false;
        boolean withScores = false;
        
        if (args.size() > 2) {
            try {
                String countStr = new String(args.get(2), StandardCharsets.UTF_8);
                // check if arg 2 is "WITHSCORES" (if count is optional? No, syntax is key [count [WITHSCORES]])
                // Redis docs: ZRANDMEMBER key [count [WITHSCORES]]
                // If 3rd arg exists, it MUST be count. WITHSCORES is 4th, or implied if 3rd is count?
                // Actually: ZRANDMEMBER key [count [WITHSCORES]]
                // If 1 arg: ZRANDMEMBER key -> return bulk string
                // If 2 args: ZRANDMEMBER key count -> return array
                // If 3 args: ZRANDMEMBER key count WITHSCORES -> return array with scores.
                
                count = Integer.parseInt(countStr);
                hasCount = true;
                
                if (args.size() > 3) {
                     String opt = new String(args.get(3), StandardCharsets.UTF_8).toUpperCase();
                     if (opt.equals("WITHSCORES")) {
                         withScores = true;
                     } else {
                         client.sendError("ERR syntax error");
                         return;
                     }
                }
            } catch (NumberFormatException e) {
                 // Might be weird if user typed ZRANDMEMBER key WITHSCORES directly? 
                 // Redis says: "count argument is optional". But if provided, it's an integer.
                 client.sendError("ERR value is not an integer or out of range");
                 return;
            }
        }
        
        List<String> keys = new ArrayList<>(zset.scores.keySet());
        
        if (!hasCount) {
            // Return single element (Bulk String)
            Random rand = new Random();
            String member = keys.get(rand.nextInt(keys.size()));
            client.sendBulkString(member);
            return;
        }
        
        // Count provided -> Return Array
        List<byte[]> result = new ArrayList<>();
        Random rand = new Random();
        
        if (count > 0) {
            // Distinct
            if (count >= keys.size()) {
                for (String m : keys) {
                    addResult(result, m, zset, withScores);
                }
            } else {
                Collections.shuffle(keys);
                for (int i = 0; i < count; i++) {
                    addResult(result, keys.get(i), zset, withScores);
                }
            }
        } else {
            // Allow duplicates (count < 0)
            int num = Math.abs(count);
            for (int i = 0; i < num; i++) {
                String m = keys.get(rand.nextInt(keys.size()));
                addResult(result, m, zset, withScores);
            }
        }
        
        client.sendResponse(Resp.array(result), null);
    }
    
    private void addResult(List<byte[]> result, String member, CaradeZSet zset, boolean withScores) {
        result.add(member.getBytes(StandardCharsets.UTF_8));
        if (withScores) {
            Double s = zset.scores.get(member);
            String scoreStr = (s == null) ? "0" : String.valueOf(s);
            if (scoreStr.endsWith(".0")) scoreStr = scoreStr.substring(0, scoreStr.length()-2);
            result.add(scoreStr.getBytes(StandardCharsets.UTF_8));
        }
    }
}
