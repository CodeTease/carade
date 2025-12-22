package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.List;

public class LTrimCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: LTRIM key start stop");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        if (entry == null) {
            client.sendSimpleString("OK");
        } else if (entry.type != DataType.LIST) {
            client.sendError("WRONGTYPE");
        } else {
            try {
                int start = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
                int stop = Integer.parseInt(new String(args.get(3), StandardCharsets.UTF_8));
                
                client.executeWrite(() -> {
                    ValueEntry e = Carade.db.get(client.getDbIndex(), key);
                    if (e != null && e.type == DataType.LIST) {
                        ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) e.getValue();
                        int size = list.size();
                        int s = start;
                        int st = stop;
                        
                        if (s < 0) s += size;
                        if (st < 0) st += size;
                        if (s < 0) s = 0;
                        
                        if (s > st || s >= size) {
                            list.clear();
                            Carade.db.remove(client.getDbIndex(), key);
                        } else {
                            if (st >= size) st = size - 1;
                            int keep = st - s + 1;
                            
                            // Remove from head
                            for (int i = 0; i < s; i++) list.pollFirst();
                            
                            // Remove from tail
                            int removeTail = list.size() - keep;
                            for (int i = 0; i < removeTail; i++) list.pollLast();
                        }
                        Carade.notifyWatchers(key);
                    }
                }, "LTRIM", key, String.valueOf(start), String.valueOf(stop));
                
                client.sendSimpleString("OK");
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not an integer or out of range");
            }
        }
    }
}
