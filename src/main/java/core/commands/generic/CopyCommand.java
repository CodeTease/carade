package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CopyCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: COPY source destination [DB destination-db] [REPLACE]");
            return;
        }
        
        String source = new String(args.get(1), StandardCharsets.UTF_8);
        String destination = new String(args.get(2), StandardCharsets.UTF_8);
        
        int targetDb = client.getDbIndex();
        boolean replace = false;
        
        for (int i = 3; i < args.size(); i++) {
            String arg = new String(args.get(i), StandardCharsets.UTF_8).toUpperCase();
            if (arg.equals("DB")) {
                if (i + 1 >= args.size()) {
                    client.sendError("ERR syntax error");
                    return;
                }
                try {
                    targetDb = Integer.parseInt(new String(args.get(++i), StandardCharsets.UTF_8));
                } catch (NumberFormatException e) {
                    client.sendError("ERR value is not an integer or out of range");
                    return;
                }
            } else if (arg.equals("REPLACE")) {
                replace = true;
            } else {
                client.sendError("ERR syntax error");
                return;
            }
        }
        
        if (targetDb < 0 || targetDb >= CaradeDatabase.DB_COUNT) {
            client.sendError("ERR DB index is out of range");
            return;
        }
        
        final int[] result = {0};
        final int finalTargetDb = targetDb;
        final boolean finalReplace = replace;
        
        client.executeWrite(() -> {
            ValueEntry val = Carade.db.get(client.getDbIndex(), source);
            if (val == null) {
                result[0] = 0;
                return;
            }
            
            if (Carade.db.exists(finalTargetDb, destination)) {
                if (!finalReplace) {
                    result[0] = 0;
                    return;
                }
            }
            
            ValueEntry newVal = val.copy();
            Carade.db.put(finalTargetDb, destination, newVal);
            Carade.notifyWatchers(destination);
            result[0] = 1;
            
        }, "COPY", source, destination);
        
        client.sendInteger(result[0]);
    }
}
