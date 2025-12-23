package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class GetDelCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 2) {
            client.sendError("usage: GETDEL key");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        
        // Check state (safe because we hold lock)
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        
        if (entry == null) {
            client.sendNull();
            return;
        }
        
        if (entry.type != DataType.STRING) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        
        byte[] val = (byte[]) entry.getValue();
        
        client.executeWrite(() -> {
            Carade.db.remove(client.getDbIndex(), key);
            Carade.notifyWatchers(key);
        }, "DEL", key);
        
        client.sendResponse(Resp.bulkString(val), new String(val, StandardCharsets.UTF_8));
    }
}
