package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LPushXCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: LPUSHX key value [value ...]");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        
        Object[] logArgs = new Object[args.size()-1];
        for(int i=1; i<args.size(); i++) logArgs[i-1] = new String(args.get(i), StandardCharsets.UTF_8);

        client.executeWrite(() -> {
            ValueEntry v = Carade.db.get(client.getDbIndex(), key);
            if (v == null) {
                client.sendInteger(0);
                return;
            }
            if (v.type != DataType.LIST) {
                throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            
            ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
            for (int i = 2; i < args.size(); i++) {
                String val = new String(args.get(i), StandardCharsets.UTF_8);
                list.addFirst(val);
            }
            v.touch();
            Carade.notifyWatchers(key);
            client.sendInteger(list.size());
            
        }, "LPUSHX", logArgs);
    }
}
