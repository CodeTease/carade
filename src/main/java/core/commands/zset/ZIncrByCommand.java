package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZIncrByCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: ZINCRBY key increment member");
            return;
        }
        Carade.performEvictionIfNeeded();
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String member = new String(args.get(3), StandardCharsets.UTF_8);
        final double[] ret = {0.0};
        try {
            double incr = Double.parseDouble(new String(args.get(2), StandardCharsets.UTF_8));
            String incrStr = new String(args.get(2), StandardCharsets.UTF_8);
            
            client.executeWrite(() -> {
                Carade.db.getStore(client.getDbIndex()).compute(key, (k, v) -> {
                    CaradeZSet zset;
                    if (v == null) {
                        zset = new CaradeZSet();
                        v = new ValueEntry(zset, DataType.ZSET, -1);
                    } else if (v.type != DataType.ZSET) {
                        throw new RuntimeException("WRONGTYPE");
                    } else {
                        zset = (CaradeZSet) v.getValue();
                    }
                    ret[0] = zset.incrBy(incr, member);
                    v.touch();
                    return v;
                });
                Carade.notifyWatchers(key);
            }, "ZINCRBY", key, incrStr, member);
            
            String s = String.valueOf(ret[0]);
            if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
            client.sendBulkString(s);
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not a valid float");
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                client.sendError(msg);
            else throw e;
        }
    }
}
