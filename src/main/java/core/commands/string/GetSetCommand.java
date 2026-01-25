package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class GetSetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("wrong number of arguments for 'getset' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        byte[] newValue = args.get(2);
        final byte[][] oldValRef = {null};

        client.executeWrite(() -> {
            Carade.db.getStore(client.dbIndex).compute(key, (k, v) -> {
                if (v == null) {
                    oldValRef[0] = null;
                    return new ValueEntry(newValue, DataType.STRING, -1);
                } else if (v.type != DataType.STRING) {
                    throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                } else {
                    oldValRef[0] = (byte[]) v.getValue();
                    ValueEntry newV = new ValueEntry(newValue, DataType.STRING, -1);
                    return newV;
                }
            });
            Carade.notifyWatchers(key);
        }, "GETSET", key, newValue);
        
        if (oldValRef[0] == null) {
            client.sendNull();
        } else {
            client.sendBulkString(new String(oldValRef[0], StandardCharsets.UTF_8));
        }
    }
}