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

public class TdInfoCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'TD.INFO' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
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
        
        List<Object> info = new ArrayList<>();
        info.add("Compression".getBytes(StandardCharsets.UTF_8));
        info.add(String.valueOf(digest.getCompression()).getBytes(StandardCharsets.UTF_8));
        info.add("Centroids".getBytes(StandardCharsets.UTF_8));
        info.add((long)digest.centroidCount()); 
        info.add("Count".getBytes(StandardCharsets.UTF_8));
        info.add(digest.size()); 
        
        client.sendMixedArray(info);
    }
}
