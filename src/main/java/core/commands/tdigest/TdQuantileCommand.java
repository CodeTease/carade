package core.commands.tdigest;

import core.commands.Command;
import core.db.CaradeDatabase;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.tdigest.TDigest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TdQuantileCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'TD.QUANTILE' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        
        client.executeWrite(() -> {
            ValueEntry entry = CaradeDatabase.getInstance().get(client.getDbIndex(), key);

            if (entry == null) {
                client.sendError("ERR key does not exist");
                return;
            }

            if (entry.type != DataType.TDIGEST) {
                client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
                return;
            }

            TDigest digest = (TDigest) entry.getValue();
            List<byte[]> results = new ArrayList<>();

            for (int i = 2; i < args.size(); i++) {
                try {
                    double q = Double.parseDouble(new String(args.get(i), StandardCharsets.UTF_8));
                    if (q < 0 || q > 1) {
                        client.sendError("ERR quantile out of range");
                        return;
                    }
                    double val = digest.quantile(q);
                    // Use standard string representation
                    results.add(String.valueOf(val).getBytes(StandardCharsets.UTF_8));
                } catch (NumberFormatException e) {
                    client.sendError("ERR value is not a valid float");
                    return;
                }
            }
            
            client.sendArray(results);
        }, "TD.QUANTILE", args.toArray());
    }
}
