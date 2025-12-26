package core.commands.hll;

import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.Carade;
import core.structs.HyperLogLog;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class PfAddCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'pfadd' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        final int[] ret = {0};
        
        // This is a write command
        client.executeWrite(() -> {
            Carade.db.getStore(client.dbIndex).compute(key, (k, v) -> {
                HyperLogLog hll;
                if (v == null) {
                    hll = new HyperLogLog();
                    v = new ValueEntry(hll, DataType.HYPERLOGLOG, -1);
                } else if (v.type != DataType.HYPERLOGLOG) {
                    throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                } else {
                    hll = (HyperLogLog) v.getValue();
                }
                
                boolean changed = false;
                for (int i = 2; i < args.size(); i++) {
                    String elem = new String(args.get(i), StandardCharsets.UTF_8);
                    if (hll.add(elem)) changed = true;
                }
                
                if (changed) {
                    ret[0] = 1;
                    v.touch(); // Also need to mark as dirty for persistence/replication? 
                    // ValueEntry doesn't have dirty flag, AOF logs the command.
                }
                
                return v;
            });
            if (ret[0] == 1) Carade.notifyWatchers(key);
        }, "PFADD", (Object[]) args.stream().skip(1).map(b -> new String(b, StandardCharsets.UTF_8)).toArray());

        client.sendInteger(ret[0]);
    }
}
