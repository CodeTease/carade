package core.commands.tdigest;

import core.commands.Command;
import core.db.CaradeDatabase;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.tdigest.TDigest;

import java.util.List;

public class TdAddCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'TD.ADD' command");
            return;
        }

        String key = new String(args.get(1));
        
        client.executeWrite(() -> {
            CaradeDatabase db = CaradeDatabase.getInstance();
            ValueEntry existingValue = db.get(client.getDbIndex(), key);
            
            TDigest digest;
            if (existingValue == null) {
                digest = new TDigest();
                db.put(client.getDbIndex(), key, new ValueEntry(digest, DataType.TDIGEST, -1));
            } else {
                if (existingValue.type != DataType.TDIGEST) {
                    client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                }
                digest = (TDigest) existingValue.getValue();
            }

            for (int i = 2; i < args.size(); i++) {
                try {
                    double val = Double.parseDouble(new String(args.get(i)));
                    digest.add(val);
                } catch (NumberFormatException e) {
                    client.sendError("ERR value is not a valid float");
                    return;
                }
            }
            
            client.sendSimpleString("OK");
        }, "TD.ADD", args.toArray());
    }
}
