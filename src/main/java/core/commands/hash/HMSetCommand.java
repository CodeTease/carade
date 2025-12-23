package core.commands.hash;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import core.structs.CaradeHash;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HMSetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // HMSET key field value [field value ...]
        if (args.size() < 4 || (args.size() - 2) % 2 != 0) {
            client.sendError("usage: HMSET key field value [field value ...]");
            return;
        }
        
        Carade.performEvictionIfNeeded();
        String key = new String(args.get(1), StandardCharsets.UTF_8);

        try {
            Object[] logArgs = new Object[args.size()-1];
            for(int i=1; i<args.size(); i++) logArgs[i-1] = new String(args.get(i), StandardCharsets.UTF_8);
            
            client.executeWrite(() -> {
                Carade.db.getStore(client.getDbIndex()).compute(key, (k, v) -> {
                    ConcurrentHashMap<String, String> map;
                    if (v == null) {
                        map = new ConcurrentHashMap<>();
                        v = new ValueEntry(map, DataType.HASH, -1);
                    } else if (v.type != DataType.HASH) {
                        throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                    } else {
                        if (v.getValue() instanceof CaradeHash) {
                            map = ((CaradeHash) v.getValue()).map;
                        } else {
                            map = (ConcurrentHashMap<String, String>) v.getValue();
                        }
                    }
                    
                    for (int i = 2; i < args.size(); i += 2) {
                        String field = new String(args.get(i), StandardCharsets.UTF_8);
                        String val = new String(args.get(i+1), StandardCharsets.UTF_8);
                        map.put(field, val);
                    }
                    
                    v.touch();
                    return v;
                });
                Carade.notifyWatchers(key);
            }, "HMSET", logArgs);
            
            client.sendSimpleString("OK");
        } catch (RuntimeException e) {
             client.sendError("ERR " + e.getMessage());
        }
    }
}
