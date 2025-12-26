package core.commands.hll;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.HyperLogLog;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PfMergeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: PFMERGE destkey sourcekey [sourcekey ...]");
            return;
        }

        String destKey = new String(args.get(1), StandardCharsets.UTF_8);

        client.executeWrite(() -> {
            HyperLogLog merged = new HyperLogLog();
            
            // Check if dest exists
            ValueEntry destVal = Carade.db.get(client.getDbIndex(), destKey);
            if (destVal != null) {
                if (destVal.type != DataType.HYPERLOGLOG) {
                    // In Redis, if dest exists and is not HLL, it returns error?
                    // Redis says: "The destination variable is created if it does not exist."
                    // If it exists, it is used as starting point. 
                    // But if it is wrong type? 
                    // "If the destination variable exists, it is treated as one of the source sets and its content is included in the result."
                    // Error if wrong type.
                    throw new RuntimeException("WRONGTYPE Key is not a valid HyperLogLog string value.");
                }
                merged.merge((HyperLogLog) destVal.getValue());
            }

            // Merge others
            for (int i = 2; i < args.size(); i++) {
                String srcKey = new String(args.get(i), StandardCharsets.UTF_8);
                ValueEntry srcVal = Carade.db.get(client.getDbIndex(), srcKey);
                if (srcVal != null) {
                    if (srcVal.type != DataType.HYPERLOGLOG) {
                         throw new RuntimeException("WRONGTYPE Key is not a valid HyperLogLog string value.");
                    }
                    merged.merge((HyperLogLog) srcVal.getValue());
                }
            }

            Carade.db.put(client.getDbIndex(), destKey, new ValueEntry(merged, DataType.HYPERLOGLOG, -1));
            client.sendSimpleString("OK");
            
        }, "PFMERGE", args.stream().map(b -> new String(b, StandardCharsets.UTF_8)).toArray());
    }
}
