package core.commands.tdigest;

import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.tdigest.TDigest;

import java.util.List;

public class TdAddCommand extends Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'TD.ADD' command");
            return;
        }

        String key = new String(args.get(1));
        
        // Execute write
        client.executeWrite(key, (db, existingValue) -> {
            TDigest digest;
            if (existingValue == null) {
                // Create new TDigest
                digest = new TDigest();
                db.put(key, new ValueEntry(DataType.TDIGEST, digest));
            } else {
                if (existingValue.type != DataType.TDIGEST) {
                    client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                }
                digest = (TDigest) existingValue.getValue();
            }

            // Parse values
            // Format: TD.ADD key val [val ...]
            int added = 0;
            for (int i = 2; i < args.size(); i++) {
                try {
                    double val = Double.parseDouble(new String(args.get(i)));
                    digest.add(val);
                    added++;
                } catch (NumberFormatException e) {
                    client.sendError("ERR value is not a valid float");
                    return;
                }
            }
            
            client.sendSimpleString("OK");
        });
    }
}
