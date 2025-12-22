package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DecrCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: DECR key");
            return;
        }
        Carade.performEvictionIfNeeded();
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        final long[] ret = {0};
        try {
            client.executeWrite(() -> {
                Carade.db.getStore(client.getDbIndex()).compute(key, (k, v) -> {
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
                    
                    val--;
                    ret[0] = val;
                    
                    ValueEntry newV = new ValueEntry(String.valueOf(val).getBytes(StandardCharsets.UTF_8), DataType.STRING, -1);
                    if (v != null) newV.expireAt = v.expireAt;
                    newV.touch();
                    return newV;
                });
                Carade.notifyWatchers(key);
            }, "DECR", key);
            
            client.sendInteger(ret[0]);
        } catch (RuntimeException e) {
            String msg = e.getMessage();
            if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                client.sendError(msg);
            else throw e;
        }
    }
}
