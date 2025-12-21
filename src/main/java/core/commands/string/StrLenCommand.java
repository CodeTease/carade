package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class StrLenCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("wrong number of arguments for 'strlen' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);

        if (entry == null) {
            client.sendInteger(0);
        } else if (entry.type != DataType.STRING) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
        } else {
            byte[] val = (byte[]) entry.getValue();
            client.sendInteger(val.length);
        }
    }
}
