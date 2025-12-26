package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ZMScoreCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: ZMSCORE key member [member ...]");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        
        // Prepare response list
        List<byte[]> response = new ArrayList<>();
        int count = args.size() - 2;

        if (entry == null || entry.type != DataType.ZSET) {
            if (entry != null && entry.type != DataType.ZSET) {
                 client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
                 return;
            }
            // Missing key -> array of nils
            for (int i = 0; i < count; i++) {
                response.add(null);
            }
        } else {
            CaradeZSet zset = (CaradeZSet) entry.getValue();
            for (int i = 2; i < args.size(); i++) {
                String member = new String(args.get(i), StandardCharsets.UTF_8);
                Double score = zset.score(member);
                if (score == null) {
                    response.add(null);
                } else {
                    String s;
                    if (score == Math.floor(score) && !Double.isInfinite(score)) {
                        s = String.valueOf(score.longValue());
                    } else {
                        s = String.valueOf(score);
                    }
                    response.add(s.getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        client.sendArray(response);
    }
}
