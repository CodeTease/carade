package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Collections;

public class LRangeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: LRANGE key start stop");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        if (entry == null) {
            client.sendArray(Collections.emptyList());
        } else if (entry.type != DataType.LIST) {
            client.sendError("WRONGTYPE");
        } else {
            try {
                ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.getValue();
                int size = list.size(); // Approximate size
                int start = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
                int end = Integer.parseInt(new String(args.get(3), StandardCharsets.UTF_8));
                
                if (start < 0) start += size;
                if (end < 0) end += size;
                if (start < 0) start = 0;
                
                List<byte[]> sub = new ArrayList<>();
                List<String> subStr = new ArrayList<>();
                if (start <= end) {
                    Iterator<String> it = list.iterator();
                    int idx = 0;
                    while (it.hasNext() && idx <= end) {
                        String s = it.next();
                        if (idx >= start) {
                            sub.add(s.getBytes(StandardCharsets.UTF_8));
                            subStr.add(s);
                        }
                        idx++;
                    }
                }
                if (client.isResp()) client.send(true, Resp.array(sub), null);
                else {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < subStr.size(); i++) sb.append((i+1) + ") \"" + subStr.get(i) + "\"\n");
                    client.send(false, null, sb.toString().trim());
                }
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not an integer or out of range");
            }
        }
    }
}
