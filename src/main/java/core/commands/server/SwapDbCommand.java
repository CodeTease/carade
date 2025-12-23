package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SwapDbCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 3) {
            client.sendError("usage: SWAPDB index1 index2");
            return;
        }
        
        int idx1, idx2;
        try {
            idx1 = Integer.parseInt(new String(args.get(1), StandardCharsets.UTF_8));
            idx2 = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }
        
        if (idx1 < 0 || idx1 >= CaradeDatabase.DB_COUNT || idx2 < 0 || idx2 >= CaradeDatabase.DB_COUNT) {
            client.sendError("ERR DB index is out of range");
            return;
        }
        
        if (idx1 == idx2) {
            client.sendSimpleString("OK");
            return;
        }
        
        client.executeWrite(() -> {
            ConcurrentHashMap<String, ValueEntry> temp = Carade.db.databases[idx1];
            Carade.db.databases[idx1] = Carade.db.databases[idx2];
            Carade.db.databases[idx2] = temp;
        }, "SWAPDB", String.valueOf(idx1), String.valueOf(idx2));
        
        client.sendSimpleString("OK");
    }
}
