package core.commands.hash;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeHash;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HRandFieldCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: HRANDFIELD key [count [WITHVALUES]]");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        long count = 1;
        boolean hasCount = false;
        boolean withValues = false;

        if (args.size() > 2) {
            try {
                count = Long.parseLong(new String(args.get(2), StandardCharsets.UTF_8));
                hasCount = true;
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not an integer or out of range");
                return;
            }
            
            if (args.size() > 3) {
                String opt = new String(args.get(3), StandardCharsets.UTF_8).toUpperCase();
                if (opt.equals("WITHVALUES")) {
                    withValues = true;
                } else {
                    client.sendError("ERR syntax error");
                    return;
                }
            }
        }
        
        ValueEntry v = Carade.db.get(client.getDbIndex(), key);
        if (v == null) {
            if (hasCount) client.sendArray(new ArrayList<>()); // Empty array
            else client.sendNull();
            return;
        }
        if (v.type != DataType.HASH) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        
        ConcurrentHashMap<String, String> map;
        if (v.getValue() instanceof CaradeHash) {
            map = ((CaradeHash) v.getValue()).map;
        } else {
            map = (ConcurrentHashMap<String, String>) v.getValue();
        }
        
        if (map.isEmpty()) {
            if (hasCount) client.sendArray(new ArrayList<>());
            else client.sendNull();
            return;
        }

        List<String> keys = new ArrayList<>(map.keySet());
        
        if (!hasCount) {
             // Return single bulk string
             if (keys.isEmpty()) { client.sendNull(); return; }
             String randKey = keys.get(new Random().nextInt(keys.size()));
             client.sendBulkString(randKey);
             return;
        }
        
        List<byte[]> response = new ArrayList<>();
        Random rand = new Random();
        
        long num = Math.abs(count);
        
        if (count > 0) {
            // Distinct
            if (num >= keys.size()) {
                // Return all
                for (String k : keys) {
                    addResult(response, k, map, withValues);
                }
            } else {
                Collections.shuffle(keys);
                for (int i = 0; i < num; i++) {
                    addResult(response, keys.get(i), map, withValues);
                }
            }
        } else {
            // Allow duplicates
            for (int i = 0; i < num; i++) {
                String k = keys.get(rand.nextInt(keys.size()));
                addResult(response, k, map, withValues);
            }
        }
        
        client.sendArray(response);
    }
    
    private void addResult(List<byte[]> response, String key, Map<String, String> map, boolean withValues) {
        response.add(key.getBytes(StandardCharsets.UTF_8));
        if (withValues) {
            String val = map.get(key);
            response.add(val == null ? new byte[0] : val.getBytes(StandardCharsets.UTF_8));
        }
    }
}
