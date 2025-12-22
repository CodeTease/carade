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

public class SetRangeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: SETRANGE key offset value");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        final int[] ret = {0};
        
        try {
            int offset = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
            byte[] val = args.get(3);
            
            if (offset < 0) {
                client.sendError("ERR offset is out of range");
                return;
            }

            client.executeWrite(() -> {
                Carade.db.getStore(client.dbIndex).compute(key, (k, v) -> {
                    byte[] current;
                    if (v == null) {
                        current = new byte[0];
                    } else if (v.type != DataType.STRING) {
                         throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                    } else {
                        current = (byte[]) v.getValue();
                    }
                    
                    int neededLen = offset + val.length;
                    byte[] newVal;
                    if (neededLen > current.length) {
                        newVal = new byte[neededLen];
                        System.arraycopy(current, 0, newVal, 0, current.length);
                    } else {
                        newVal = new byte[current.length];
                        System.arraycopy(current, 0, newVal, 0, current.length);
                    }
                    
                    System.arraycopy(val, 0, newVal, offset, val.length);
                    
                    if (v == null) {
                        v = new ValueEntry(newVal, DataType.STRING, -1);
                    } else {
                        v.setValue(newVal);
                        v.touch();
                    }
                    ret[0] = newVal.length;
                    return v;
                });
                Carade.notifyWatchers(key);
            }, "SETRANGE", key, String.valueOf(offset), new String(val, StandardCharsets.UTF_8));

            client.sendResponse(Resp.integer(ret[0]), "(integer) " + ret[0]);
            
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        } catch (RuntimeException e) {
             client.sendError(e.getMessage());
        }
    }
}
