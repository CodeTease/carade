package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZScoreCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: ZSCORE key member");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String member = new String(args.get(2), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
         if (entry == null || entry.type != DataType.ZSET) {
             if (entry != null && entry.type != DataType.ZSET) client.sendError("WRONGTYPE");
             else client.sendNull();
         } else {
             CaradeZSet zset = (CaradeZSet) entry.getValue();
             Double score = zset.score(member);
             if (score == null) client.sendNull();
             else {
                 String s = String.valueOf(score);
                 if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                 client.sendBulkString(s);
             }
         }
    }
}
