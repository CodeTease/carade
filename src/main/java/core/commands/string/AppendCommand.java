package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import core.server.WriteSequencer;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class AppendCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: APPEND key value");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        byte[] val = args.get(2);
        
        final long[] ret = {0};
        
        try {
            client.executeWrite(() -> {
                Carade.db.getStore(client.dbIndex).compute(key, (k, v) -> {
                    if (v == null) {
                        v = new ValueEntry(val, DataType.STRING, -1);
                        ret[0] = val.length;
                        return v;
                    } else if (v.type != DataType.STRING) {
                        throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                    } else {
                        byte[] oldVal = (byte[]) v.getValue();
                        byte[] newVal = new byte[oldVal.length + val.length];
                        System.arraycopy(oldVal, 0, newVal, 0, oldVal.length);
                        System.arraycopy(val, 0, newVal, oldVal.length, val.length);
                        v.setValue(newVal);
                        v.touch();
                        ret[0] = newVal.length;
                        return v;
                    }
                });
                Carade.notifyWatchers(key);
            }, "APPEND", key, new String(val, StandardCharsets.UTF_8));

            client.sendResponse(Resp.integer(ret[0]), "(integer) " + ret[0]);
        } catch (RuntimeException e) {
            client.sendError(e.getMessage());
        }
    }
}
