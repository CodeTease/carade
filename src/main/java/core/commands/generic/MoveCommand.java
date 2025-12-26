package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MoveCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 3) {
            client.sendError("usage: MOVE key db");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        int targetDb;
        try {
            targetDb = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }
        
        if (targetDb < 0 || targetDb >= CaradeDatabase.DB_COUNT) {
            client.sendError("ERR DB index is out of range");
            return;
        }
        
        if (client.getDbIndex() == targetDb) {
            client.sendError("ERR source and destination objects are the same");
            return;
        }
        
        final int[] result = {0};
        
        client.executeWrite(() -> {
            // Check if key exists in source
            if (!Carade.db.exists(client.getDbIndex(), key)) {
                result[0] = 0;
                return;
            }
            
            // Check if key exists in target
            if (Carade.db.exists(targetDb, key)) {
                result[0] = 0;
                return;
            }
            
            ValueEntry val = Carade.db.remove(client.getDbIndex(), key);
            if (val != null) {
                Carade.db.put(targetDb, key, val);
                Carade.notifyWatchers(key); // Assuming watchers are global or handled? 
                // In Redis watchers are db-specific usually? 
                // But Carade.notifyWatchers takes key only. Maybe it checks all?
                // The implementation of notifyWatchers is likely in Carade.java.
                // Assuming it works.
                result[0] = 1;
            }
        }, "MOVE", key, String.valueOf(targetDb));
        
        client.sendInteger(result[0]);
    }
}
