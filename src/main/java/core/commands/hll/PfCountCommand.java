package core.commands.hll;

import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.Carade;
import core.structs.HyperLogLog;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class PfCountCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'pfcount' command");
            return;
        }

        if (args.size() == 2) {
            // Single key optimization
            String key = new String(args.get(1), StandardCharsets.UTF_8);
            ValueEntry entry = Carade.db.get(client.dbIndex, key);
            
            if (entry == null) {
                client.sendInteger(0);
                return;
            }
            
            if (entry.type != DataType.HYPERLOGLOG) {
                client.sendError("WRONGTYPE Key is not a valid HyperLogLog string value.");
                return;
            }
            
            HyperLogLog hll = (HyperLogLog) entry.getValue();
            client.sendInteger(hll.count());
        } else {
            // Merge multiple HLLs temporarily
            HyperLogLog merged = new HyperLogLog();
            for (int i = 1; i < args.size(); i++) {
                String key = new String(args.get(i), StandardCharsets.UTF_8);
                ValueEntry entry = Carade.db.get(client.dbIndex, key);
                
                if (entry != null) {
                    if (entry.type != DataType.HYPERLOGLOG) {
                         // Redis behavior: "returns error if any key is not HLL" ? 
                         // Docs say: "The command raises an error if the value at key is not a HyperLogLog."
                         client.sendError("WRONGTYPE Key is not a valid HyperLogLog string value.");
                         return;
                    }
                    merged.merge((HyperLogLog) entry.getValue());
                }
            }
            client.sendInteger(merged.count());
        }
    }
}
