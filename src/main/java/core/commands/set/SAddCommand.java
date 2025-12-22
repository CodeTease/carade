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

public class SAddCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: SADD key member");
            return;
        }
        
        Carade.performEvictionIfNeeded();
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String member = new String(args.get(2), StandardCharsets.UTF_8);
        final int[] ret = {0};
        
        try {
            client.executeWrite(() -> {
                Carade.db.getStore(client.getDbIndex()).compute(key, (k, v) -> {
                    if (v == null) {
                        Set<String> set = ConcurrentHashMap.newKeySet();
                        set.add(member);
                        ret[0] = 1;
                        return new ValueEntry(set, DataType.SET, -1);
                    } else if (v.type != DataType.SET) {
                        throw new RuntimeException("WRONGTYPE");
                    } else {
                        Set<String> set = (Set<String>) v.getValue();
                        if (set.add(member)) ret[0] = 1;
                        else ret[0] = 0;
                        v.touch();
                        return v;
                    }
                });
                if (ret[0] == 1) Carade.notifyWatchers(key);
            }, "SADD", key, member);
            
            client.sendInteger(ret[0]);
        } catch (RuntimeException e) {
            client.sendError("WRONGTYPE");
        }
    }
}
