package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PersistCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendResponse(Resp.error("usage: PERSIST key"), "(error) usage: PERSIST key");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);
        if (entry == null) {
            client.sendResponse(Resp.integer(0), "(integer) 0");
        } else if (entry.getExpireAt() == -1) {
            client.sendResponse(Resp.integer(0), "(integer) 0");
        } else {
            entry.setExpireAt(-1);
            if (Carade.aofHandler != null) {
                Carade.aofHandler.log("PERSIST", key);
            }
            client.sendResponse(Resp.integer(1), "(integer) 1");
        }
    }
}
