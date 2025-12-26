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

public class BfMAddCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: BF.MADD key item [item ...]");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        List<Object> results = new ArrayList<>();
        
        try {
            client.executeWrite(() -> {
                Carade.db.getStore(client.getDbIndex()).compute(key, (k, v) -> {
                    BloomFilter bf;
                    if (v == null) {
                        bf = new BloomFilter();
                        v = new ValueEntry(bf, DataType.BLOOM, -1);
                    } else if (v.type != DataType.BLOOM) {
                         throw new RuntimeException("WRONGTYPE");
                    } else {
                        bf = (BloomFilter) v.getValue();
                        v.touch();
                    }
                    
                    for (int i = 2; i < args.size(); i++) {
                        String item = new String(args.get(i), StandardCharsets.UTF_8);
                        results.add(bf.add(item));
                    }
                    
                    return v;
                });
                Carade.notifyWatchers(key);
                
            }, "BF.MADD", args.subList(1, args.size()).toArray());
            
            client.sendResponse(Resp.mixedArray(results), null);
            
        } catch (RuntimeException e) {
            client.sendError(e.getMessage());
        }
    }
}
