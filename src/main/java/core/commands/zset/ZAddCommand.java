package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZAddCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4 || (args.size() - 2) % 2 != 0) {
            client.sendError("usage: ZADD key score member [score member ...]");
            return;
        }

        Carade.performEvictionIfNeeded();
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        final int[] addedCount = {0};
        
        try {
            String[] cmdArgs = new String[args.size()-1];
            for(int i=1; i<args.size(); i++) cmdArgs[i-1] = new String(args.get(i), StandardCharsets.UTF_8);
            
            client.executeWrite(() -> {
                Carade.db.getStore(client.getDbIndex()).compute(key, (k, v) -> {
                    CaradeZSet zset;
                    if (v == null) {
                        zset = new CaradeZSet();
                        v = new ValueEntry(zset, DataType.ZSET, -1);
                    } else if (v.type != DataType.ZSET) {
                        throw new RuntimeException("WRONGTYPE");
                    } else {
                        zset = (CaradeZSet) v.getValue();
                    }
                    
                    for (int i = 2; i < args.size(); i += 2) {
                        try {
                            double score = Double.parseDouble(new String(args.get(i), StandardCharsets.UTF_8));
                            String member = new String(args.get(i+1), StandardCharsets.UTF_8);
                            addedCount[0] += zset.add(score, member);
                        } catch (Exception ex) {}
                    }
                    v.touch();
                    return v;
                });
                Carade.notifyWatchers(key);
                Carade.checkBlockers(key);
            }, "ZADD", (Object[]) cmdArgs);
            
            client.sendInteger(addedCount[0]);
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                client.sendError(msg);
            else throw e;
        }
    }
}
