package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZCardCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: ZCARD key");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        if (entry == null || entry.type != DataType.ZSET) {
            if (entry != null && entry.type != DataType.ZSET) client.sendError("WRONGTYPE");
            else client.sendInteger(0);
        } else {
            CaradeZSet zset = (CaradeZSet) entry.getValue();
            client.sendInteger(zset.size());
        }
    }
}
