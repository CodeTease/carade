package core.commands.bloom;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.BloomFilter;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class BfExistsCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: BF.EXISTS key item");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String item = new String(args.get(2), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        if (entry == null) client.sendInteger(0);
        else if (entry.type != DataType.BLOOM) client.sendError("WRONGTYPE");
        else {
            BloomFilter bf = (BloomFilter) entry.getValue();
            client.sendInteger(bf.exists(item));
        }
    }
}
