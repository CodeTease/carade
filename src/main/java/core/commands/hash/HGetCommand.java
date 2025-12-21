package core.commands.hash;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HGetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: HGET key field");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String field = new String(args.get(2), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);
        if (entry == null || entry.type != DataType.HASH) {
            client.sendResponse(Resp.bulkString((byte[])null), "(nil)");
        } else {
            ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) entry.getValue();
            String val = map.get(field);
            client.sendResponse(Resp.bulkString(val != null ? val.getBytes(StandardCharsets.UTF_8) : null), val != null ? val : "(nil)");
        }
    }
}
