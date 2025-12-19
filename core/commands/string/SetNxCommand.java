package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SetNxCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: SETNX key value");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        byte[] val = args.get(2);

        if (Carade.db.exists(key)) {
            client.sendResponse(Resp.integer(0), "(integer) 0");
            return;
        }

        Carade.performEvictionIfNeeded();
        Carade.db.put(key, new ValueEntry(val, DataType.STRING, -1));
        Carade.notifyWatchers(key);
        Carade.aofHandler.log("SETNX", key, val);
        
        client.sendResponse(Resp.integer(1), "(integer) 1");
    }
}
