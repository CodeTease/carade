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

public class HStrLenCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 3) {
            client.sendError("usage: HSTRLEN key field");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String field = new String(args.get(2), StandardCharsets.UTF_8);
        
        ValueEntry v = Carade.db.get(client.getDbIndex(), key);
        if (v == null) {
            client.sendInteger(0);
            return;
        }
        if (v.type != DataType.HASH) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        
        ConcurrentHashMap<String, String> map;
        if (v.getValue() instanceof CaradeHash) {
            map = ((CaradeHash) v.getValue()).map;
        } else {
            map = (ConcurrentHashMap<String, String>) v.getValue();
        }
        
        String val = map.get(field);
        if (val == null) {
             client.sendInteger(0);
        } else {
             client.sendInteger(val.length());
        }
    }
}
