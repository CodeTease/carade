package core.commands.set;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SMoveCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: SMOVE source destination member");
            return;
        }
        
        String source = new String(args.get(1), StandardCharsets.UTF_8);
        String destination = new String(args.get(2), StandardCharsets.UTF_8);
        String member = new String(args.get(3), StandardCharsets.UTF_8);
        
        final int[] result = {0};
        
        try {
            client.executeWrite(() -> {
                ValueEntry srcVal = Carade.db.get(client.dbIndex, source);
                if (srcVal != null && srcVal.type != DataType.SET) {
                    throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                
                ValueEntry destVal = Carade.db.get(client.dbIndex, destination);
                if (destVal != null && destVal.type != DataType.SET) {
                    throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                
                if (srcVal == null) {
                    result[0] = 0;
                    return;
                }
                
                Set<String> srcSet = (Set<String>) srcVal.getValue();
                if (!srcSet.contains(member)) {
                    result[0] = 0;
                    return;
                }
                
                // Remove from source
                srcSet.remove(member);
                if (srcSet.isEmpty()) {
                    Carade.db.remove(client.dbIndex, source);
                }
                
                // Add to destination
                Carade.db.getStore(client.dbIndex).compute(destination, (k, v) -> {
                    if (v == null) {
                        Set<String> set = ConcurrentHashMap.newKeySet();
                        set.add(member);
                        return new ValueEntry(set, DataType.SET, -1);
                    } else {
                        // We already checked type above
                        ((Set<String>) v.getValue()).add(member);
                        return v;
                    }
                });
                
                result[0] = 1;
                Carade.notifyWatchers(source);
                Carade.notifyWatchers(destination);
                
            }, "SMOVE", source, destination, member);
            
            client.sendInteger(result[0]);
            
        } catch (RuntimeException e) {
            client.sendError(e.getMessage());
        }
    }
}
