package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RenameNxCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: RENAMENX key newkey");
            return;
        }
        String oldKey = new String(args.get(1), StandardCharsets.UTF_8);
        String newKey = new String(args.get(2), StandardCharsets.UTF_8);
        
        final int[] result = {0}; // 0 = no op, 1 = renamed, -1 = error no such key
        
        client.executeWrite(() -> {
            if (Carade.db.exists(client.getDbIndex(), newKey)) {
                result[0] = 0;
            } else {
                ValueEntry val = Carade.db.remove(client.getDbIndex(), oldKey);
                if (val != null) {
                    Carade.db.put(client.getDbIndex(), newKey, val);
                    Carade.notifyWatchers(oldKey);
                    Carade.notifyWatchers(newKey);
                    result[0] = 1;
                } else {
                    result[0] = -1;
                }
            }
        }, "RENAMENX", oldKey, newKey);

        if (result[0] == -1) {
            client.sendError("ERR no such key");
        } else {
            client.sendInteger(result[0]);
        }
    }
}
