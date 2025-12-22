package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ZPopMaxCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: ZPOPMAX key [count]");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        int count = 1;
        if (args.size() > 2) {
            try {
                count = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not an integer or out of range");
                return;
            }
        }
        
        final int finalCount = count;
        final List<byte[]> result = new ArrayList<>();
        
        client.executeWrite(() -> {
             ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
             if (entry != null && entry.type == DataType.ZSET) {
                 CaradeZSet zset = (CaradeZSet) entry.getValue();
                 List<ZNode> popped = zset.popMax(finalCount);
                 for (ZNode node : popped) {
                     result.add(node.member.getBytes(StandardCharsets.UTF_8));
                     String s = String.valueOf(node.score);
                     if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                     result.add(s.getBytes(StandardCharsets.UTF_8));
                 }
                 if (!popped.isEmpty()) Carade.notifyWatchers(key);
                 if (zset.size() == 0) Carade.db.remove(client.getDbIndex(), key);
             }
        }, "ZPOPMAX", key, String.valueOf(count));
        
        client.sendArray(result);
    }
}
