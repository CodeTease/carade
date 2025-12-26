package core.commands.hash;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HIncrByFloatCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: HINCRBYFLOAT key field increment");
            return;
        }

        Carade.performEvictionIfNeeded();
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String field = new String(args.get(2), StandardCharsets.UTF_8);
        final double[] ret = {0.0};
        
        try {
            double incr = Double.parseDouble(new String(args.get(3), StandardCharsets.UTF_8));
            String incrStr = new String(args.get(3), StandardCharsets.UTF_8);
            
            client.executeWrite(() -> {
                Carade.db.getStore(client.getDbIndex()).compute(key, (k, v) -> {
                    if (v == null) {
                        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
                        map.put(field, String.valueOf(incr));
                        ret[0] = incr;
                        return new ValueEntry(map, DataType.HASH, -1);
                    } else if (v.type != DataType.HASH) {
                        throw new RuntimeException("WRONGTYPE");
                    } else {
                        ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.getValue();
                        map.compute(field, (f, val) -> {
                            double oldVal = 0;
                            if (val != null) {
                                try { oldVal = Double.parseDouble(val); } catch (Exception e) { throw new RuntimeException("ERR hash value is not a float"); }
                            }
                            double newVal = oldVal + incr;
                            ret[0] = newVal;
                            return String.valueOf(newVal);
                        });
                        v.touch();
                        return v;
                    }
                });
                Carade.notifyWatchers(key);
            }, "HINCRBYFLOAT", key, field, incrStr);
            
            String s = formatDouble(ret[0]);
            client.sendBulkString(s);
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not a float");
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                client.sendError(msg);
            else throw e;
        }
    }
    
    private String formatDouble(double d) {
        if (d == (long) d) return String.format("%d", (long) d);
        else return String.format("%s", d);
    }
}
