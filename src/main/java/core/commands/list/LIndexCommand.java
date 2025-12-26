package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LIndexCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("wrong number of arguments for 'lindex' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        int index;
        try {
            index = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }

        ValueEntry entry = Carade.db.get(client.dbIndex, key);
        if (entry == null) {
            client.sendNull();
        } else if (entry.type != DataType.LIST) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
        } else {
            ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.getValue();
            int size = list.size();
            
            if (index < 0) index += size;
            
            if (index < 0 || index >= size) {
                client.sendNull();
                return;
            }
            
            // Access by iteration
            // Optimization: if index is closer to end, use descending iterator?
            // ConcurrentLinkedDeque iterator is weakly consistent.
            // But descending iterator also works.
            
            String val = null;
            if (index < size / 2) {
                Iterator<String> it = list.iterator();
                for (int i = 0; i <= index && it.hasNext(); i++) {
                    val = it.next();
                }
            } else {
                Iterator<String> it = list.descendingIterator();
                int target = size - 1 - index;
                for (int i = 0; i <= target && it.hasNext(); i++) {
                    val = it.next();
                }
            }
            
            if (val != null) {
                client.sendBulkString(val);
            } else {
                client.sendNull(); // Should not happen if size check was correct but race condition possible
            }
        }
    }
}
