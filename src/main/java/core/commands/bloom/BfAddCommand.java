package core.commands.bloom;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.BloomFilter;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class BfAddCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: BF.ADD key item");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String item = new String(args.get(2), StandardCharsets.UTF_8);
        final int[] ret = {0};
        
        client.executeWrite(() -> {
            Carade.db.getStore(client.getDbIndex()).compute(key, (k, v) -> {
                if (v == null) {
                    BloomFilter bf = new BloomFilter();
                    ret[0] = bf.add(item);
                    return new ValueEntry(bf, DataType.BLOOM, -1);
                } else if (v.type != DataType.BLOOM) {
                    throw new RuntimeException("WRONGTYPE");
                } else {
                    BloomFilter bf = (BloomFilter) v.getValue();
                    ret[0] = bf.add(item);
                    v.touch();
                    return v;
                }
            });
            if (ret[0] == 1) Carade.notifyWatchers(key);
        }, "BF.ADD", key, item);
        
        client.sendInteger(ret[0]);
    }
}
