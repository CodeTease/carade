package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class RPushCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: RPUSH key value [value ...]");
            return;
        }
        Carade.performEvictionIfNeeded();
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        try {
            Object[] logArgs = new Object[args.size()-1];
            for(int i=1; i<args.size(); i++) logArgs[i-1] = new String(args.get(i), StandardCharsets.UTF_8);

            client.executeWrite(() -> {
                Carade.db.getStore(client.getDbIndex()).compute(key, (k, v) -> {
                    ConcurrentLinkedDeque<String> list;
                    if (v == null) {
                        list = new ConcurrentLinkedDeque<>();
                        v = new ValueEntry(list, DataType.LIST, -1);
                    } else if (v.type != DataType.LIST) {
                        throw new RuntimeException("WRONGTYPE");
                    } else {
                        list = (ConcurrentLinkedDeque<String>) v.getValue();
                    }
                    
                    for (int i = 2; i < args.size(); i++) {
                        String val = new String(args.get(i), StandardCharsets.UTF_8);
                        list.addLast(val);
                    }
                    
                    v.touch(); // Update LRU
                    return v;
                });
                Carade.notifyWatchers(key);
                Carade.checkBlockers(key); // Notify waiters
            }, "RPUSH", logArgs);
            
            ValueEntry v = Carade.db.get(client.getDbIndex(), key);
            int size = (v != null && v.getValue() instanceof Deque) ? ((Deque)v.getValue()).size() : 0;
            client.sendInteger(size);
        } catch (RuntimeException e) {
            client.sendError("WRONGTYPE");
        }
    }
}
