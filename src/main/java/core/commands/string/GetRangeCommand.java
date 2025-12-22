package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class GetRangeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: GETRANGE key start end");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);

        if (entry == null) {
            client.sendResponse(Resp.bulkString("".getBytes(StandardCharsets.UTF_8)), "\"\"");
            return;
        }
        
        if (entry.type != DataType.STRING) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }

        try {
            int start = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
            int end = Integer.parseInt(new String(args.get(3), StandardCharsets.UTF_8));
            
            byte[] val = (byte[]) entry.getValue();
            int len = val.length;
            
            if (start < 0) start += len;
            if (end < 0) end += len;
            
            if (start < 0) start = 0;
            if (end < 0) end = 0;
            if (end >= len) end = len - 1;
            
            if (start > end || len == 0) {
                client.sendResponse(Resp.bulkString("".getBytes(StandardCharsets.UTF_8)), "\"\"");
                return;
            }
            
            byte[] sub = new byte[end - start + 1];
            System.arraycopy(val, start, sub, 0, sub.length);
            client.sendResponse(Resp.bulkString(sub), new String(sub, StandardCharsets.UTF_8));
            
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
