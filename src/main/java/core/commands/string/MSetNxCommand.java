package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MSetNxCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3 || (args.size() - 1) % 2 != 0) {
            client.sendError("wrong number of arguments for 'msetnx' command");
            return;
        }
        Carade.performEvictionIfNeeded();

        // Check existence (Read phase - can be done outside write lock if we accept race, 
        // but for strict atomicity MSETNX check+set must be atomic. 
        // However, standard Redis logic: "Atomic" means checking and setting happens together.
        // So we should do it inside executeWrite to be safe? 
        // But checking efficiently without lock is better. 
        // Actually, if we want MSETNX to be truly atomic, we must do the check INSIDE the write lock 
        // or hold a lock. executeWrite holds the lock.
        
        // However, passing a lambda to executeWrite that might fail (return 0) is fine.
        
        Object[] logArgs = new Object[args.size() - 1];
        for(int i=1; i<args.size(); i++) {
            if ((i-1) % 2 == 0) logArgs[i-1] = new String(args.get(i), StandardCharsets.UTF_8);
            else logArgs[i-1] = args.get(i);
        }

        client.executeWrite(() -> {
            // Check existence
            for (int i = 1; i < args.size(); i += 2) {
                String key = new String(args.get(i), StandardCharsets.UTF_8);
                if (Carade.db.exists(client.getDbIndex(), key)) {
                    client.sendInteger(0);
                    return;
                }
            }
            
            // Set all
            for (int i = 1; i < args.size(); i += 2) {
                String key = new String(args.get(i), StandardCharsets.UTF_8);
                byte[] val = args.get(i + 1);
                Carade.db.put(client.getDbIndex(), key, new ValueEntry(val, DataType.STRING, -1));
                Carade.notifyWatchers(key);
            }
            client.sendInteger(1);
        }, "MSETNX", logArgs);
    }
}
