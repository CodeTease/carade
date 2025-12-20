package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class GetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: GET key");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);
        
        if (entry == null) {
            client.sendResponse(Resp.bulkString((byte[])null), "(nil)");
        } else { 
            if (entry.type != DataType.STRING && entry.type != null) {
                client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            } else {
                byte[] v = (byte[]) entry.getValue();
                client.sendResponse(Resp.bulkString(v), new String(v, StandardCharsets.UTF_8));
            }
        }
    }
}
