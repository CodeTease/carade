package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class IncrByFloatCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: INCRBYFLOAT key increment");
            return;
        }

        Carade.performEvictionIfNeeded();
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        final double[] ret = {0.0};

        try {
            double incr = Double.parseDouble(new String(args.get(2), StandardCharsets.UTF_8));
            String incrStr = new String(args.get(2), StandardCharsets.UTF_8);

            client.executeWrite(() -> {
                Carade.db.getStore(client.dbIndex).compute(key, (k, v) -> {
                    double val = 0;
                    if (v == null) {
                        val = 0;
                    } else if (v.type != DataType.STRING) {
                        throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                    } else {
                        try {
                            val = Double.parseDouble(new String((byte[])v.getValue(), StandardCharsets.UTF_8));
                        } catch (NumberFormatException e) {
                            throw new RuntimeException("ERR value is not a valid float");
                        }
                    }
                    
                    val += incr;
                    ret[0] = val;
                    
                    // Format appropriately? Redis often avoids scientific notation for simple cases, but Double.toString() is okay for MVP
                    ValueEntry newV = new ValueEntry(String.valueOf(val).getBytes(StandardCharsets.UTF_8), DataType.STRING, -1);
                    if (v != null) newV.expireAt = v.expireAt;
                    newV.touch();
                    return newV;
                });
                Carade.notifyWatchers(key);
            }, "INCRBYFLOAT", key, incrStr);
            
            client.sendResponse(Resp.bulkString(String.valueOf(ret[0])), null);
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
