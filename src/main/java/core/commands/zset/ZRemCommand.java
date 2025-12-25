package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZRemCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
         if (args.size() < 3) {
             client.sendError("usage: ZREM key member");
             return;
         }
         
         String key = new String(args.get(1), StandardCharsets.UTF_8);
         String member = new String(args.get(2), StandardCharsets.UTF_8);
         final int[] ret = {0};
         
         client.executeWrite(() -> {
             Carade.db.getStore(client.getDbIndex()).computeIfPresent(key, (k, v) -> {
                 if (v.type == DataType.ZSET) {
                     CaradeZSet zset = (CaradeZSet) v.getValue();
                     Double score = zset.scores.remove(member);
                     if (score != null) {
                         zset.sorted.remove(new ZNode(score, member));
                         ret[0] = 1;
                     }
                     if (zset.scores.isEmpty()) return null;
                 }
                 return v;
             });
             if (ret[0] == 1) {
                 Carade.notifyWatchers(key);
             }
         }, "ZREM", key, member);

         client.sendInteger(ret[0]);
    }
}
