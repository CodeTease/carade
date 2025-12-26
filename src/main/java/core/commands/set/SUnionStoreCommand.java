package core.commands.set;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SUnionStoreCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: SUNIONSTORE destination key [key ...]");
            return;
        }

        String destination = new String(args.get(1), StandardCharsets.UTF_8);
        final int[] sizeRef = {0};
        
        Object[] cmdArgs = new Object[args.size()-1];
        for(int i=1; i<args.size(); i++) cmdArgs[i-1] = new String(args.get(i), StandardCharsets.UTF_8);
        
        client.executeWrite(() -> {
            Set<String> res = new HashSet<>();
            for (int i = 2; i < args.size(); i++) {
                ValueEntry e = Carade.db.get(client.getDbIndex(), new String(args.get(i), StandardCharsets.UTF_8));
                if (e != null && e.type == DataType.SET) {
                    res.addAll((Set<String>) e.getValue());
                }
            }
            
            if (res.isEmpty()) {
                Carade.db.remove(client.getDbIndex(), destination);
            } else {
                Set<String> newSet = ConcurrentHashMap.newKeySet();
                newSet.addAll(res);
                Carade.db.put(client.getDbIndex(), destination, new ValueEntry(newSet, DataType.SET, -1));
            }
            sizeRef[0] = res.size();
            Carade.notifyWatchers(destination);
        }, "SUNIONSTORE", cmdArgs);
        
        client.sendInteger(sizeRef[0]);
    }
}
