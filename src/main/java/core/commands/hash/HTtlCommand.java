package core.commands.hash;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeHash;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HTtlCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // HTTL key field
        if (args.size() < 3) {
            client.sendError("usage: HTTL key field");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String field = new String(args.get(2), StandardCharsets.UTF_8);
        
        ValueEntry v = Carade.db.get(client.getDbIndex(), key);
        if (v == null || v.type != DataType.HASH) {
            client.sendInteger(-2);
            return;
        }
        
        CaradeHash hash;
        if (v.getValue() instanceof CaradeHash) {
             hash = (CaradeHash) v.getValue();
        } else {
             hash = new CaradeHash((ConcurrentHashMap<String, String>) v.getValue());
             v.setValue(hash);
        }
        
        if (!hash.map.containsKey(field)) {
            client.sendInteger(-2);
            return;
        }
        
        long expiry = hash.getExpiry(field);
        if (expiry == -1) {
            client.sendInteger(-1);
        } else {
            long ttl = expiry - System.currentTimeMillis();
            client.sendInteger(ttl > 0 ? ttl / 1000 : 0);
        }
    }
}
