package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RenameCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: RENAME key newkey");
            return;
        }
        String oldKey = new String(args.get(1), StandardCharsets.UTF_8);
        String newKey = new String(args.get(2), StandardCharsets.UTF_8);
        
        final int[] success = {0};
        client.executeWrite(() -> {
            ValueEntry val = Carade.db.remove(client.getDbIndex(), oldKey);
            if (val != null) {
                Carade.db.put(client.getDbIndex(), newKey, val);
                Carade.notifyWatchers(oldKey);
                Carade.notifyWatchers(newKey);
                success[0] = 1;
            }
        }, "RENAME", oldKey, newKey);

        if (success[0] == 0) {
            client.sendError("ERR no such key");
        } else {
            client.sendSimpleString("OK");
        }
    }
}
