package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DecrByCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: DECRBY key decrement");
            return;
        }

        Carade.performEvictionIfNeeded();
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        final long[] ret = {0};

        try {
            long decr = Long.parseLong(new String(args.get(2), StandardCharsets.UTF_8));
            String decrStr = new String(args.get(2), StandardCharsets.UTF_8);

            client.executeWrite(() -> {
                Carade.db.getStore(client.dbIndex).compute(key, (k, v) -> {
                    long val = 0;
                    if (v == null) {
                        val = 0;
                    } else if (v.type != DataType.STRING) {
                        throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                    } else {
                        try {
                            val = Long.parseLong(new String((byte[])v.getValue(), StandardCharsets.UTF_8));
                        } catch (NumberFormatException e) {
                            throw new RuntimeException("ERR value is not an integer or out of range");
                        }
                    }
                    
                    val -= decr;
                    ret[0] = val;
                    
                    ValueEntry newV = new ValueEntry(String.valueOf(val).getBytes(StandardCharsets.UTF_8), DataType.STRING, -1);
                    if (v != null) newV.expireAt = v.expireAt;
                    newV.touch();
                    return newV;
                });
                Carade.notifyWatchers(key);
            }, "DECRBY", key, decrStr);
            
            client.sendResponse(Resp.integer(ret[0]), "(integer) " + ret[0]);
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                client.sendError(msg);
            else throw e;
        }
    }
}