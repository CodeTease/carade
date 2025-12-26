package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZCountCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: ZCOUNT key min max");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        if (entry == null || entry.type != DataType.ZSET) {
             if (entry != null && entry.type != DataType.ZSET) client.sendError("WRONGTYPE");
             else client.sendInteger(0);
        } else {
             try {
                 String minStr = new String(args.get(2), StandardCharsets.UTF_8).toLowerCase();
                 String maxStr = new String(args.get(3), StandardCharsets.UTF_8).toLowerCase();
                 double min = minStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (minStr.equals("+inf") || minStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(minStr));
                 double max = maxStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (maxStr.equals("+inf") || maxStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(maxStr));
                 
                 CaradeZSet zset = (CaradeZSet) entry.getValue();
                 long count = 0;
                 ZNode startNode = new ZNode(min, "");
                 for (ZNode node : zset.sorted.tailSet(startNode)) {
                     if (node.score > max) break;
                     count++;
                 }
                 client.sendInteger(count);
             } catch (NumberFormatException e) {
                 client.sendError("ERR min or max is not a float");
             }
        }
    }
}
