package core.commands.bloom;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.BloomFilter;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BfMExistsCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: BF.MEXISTS key item [item ...]");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        
        List<Object> results = new ArrayList<>();
        
        if (entry == null) {
            for (int i = 2; i < args.size(); i++) results.add(0);
        } else if (entry.type != DataType.BLOOM) {
            client.sendError("WRONGTYPE");
            return;
        } else {
            BloomFilter bf = (BloomFilter) entry.getValue();
            for (int i = 2; i < args.size(); i++) {
                String item = new String(args.get(i), StandardCharsets.UTF_8);
                results.add(bf.exists(item));
            }
        }
        
        // Use Resp.mixedArray to send array of integers
        client.sendResponse(Resp.mixedArray(results), null);
    }
}
