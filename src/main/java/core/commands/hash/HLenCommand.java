package core.commands.hash;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class HLenCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("wrong number of arguments for 'hlen' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);

        if (entry == null) {
            client.sendInteger(0);
        } else if (entry.type != DataType.HASH) {
            client.sendInteger(0);
        } else {
            Map<?, ?> map = (Map<?, ?>) entry.getValue();
            client.sendInteger(map.size());
        }
    }
}
