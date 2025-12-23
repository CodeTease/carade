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

public class ZRevRankCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: ZREVRANK key member");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String member = new String(args.get(2), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        if (entry == null || entry.type != DataType.ZSET) {
                if (entry != null) client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
                else client.sendNull();
        } else {
            CaradeZSet zset = (CaradeZSet) entry.getValue();
            Double score = zset.score(member);
            if (score == null) {
                client.sendNull();
            } else {
                // O(N) scan using descendingSet
                int rank = 0;
                for (ZNode node : zset.sorted.descendingSet()) {
                    if (node.member.equals(member)) break;
                    rank++;
                }
                client.sendInteger(rank);
            }
        }
    }
}
