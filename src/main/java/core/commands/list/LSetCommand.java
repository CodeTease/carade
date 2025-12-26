package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LSetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 4) {
            client.sendError("usage: LSET key index element");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        int index;
        try {
            index = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }
        String newVal = new String(args.get(3), StandardCharsets.UTF_8);

        Object[] logArgs = new Object[]{key, String.valueOf(index), newVal};

        client.executeWrite(() -> {
            ValueEntry v = Carade.db.get(client.getDbIndex(), key);
            if (v == null) {
                throw new RuntimeException("ERR no such key");
            }
            if (v.type != DataType.LIST) {
                throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            
            ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
            int size = list.size();
            int idx = index;
            if (idx < 0) idx = size + idx;
            
            if (idx < 0 || idx >= size) {
                throw new RuntimeException("ERR index out of range");
            }
            
            // Rebuild list to set value
            ConcurrentLinkedDeque<String> newList = new ConcurrentLinkedDeque<>();
            int current = 0;
            for (String s : list) {
                if (current == idx) newList.add(newVal);
                else newList.add(s);
                current++;
            }
            v.setValue(newList);
            v.touch();
            Carade.notifyWatchers(key);
            client.sendSimpleString("OK");
            
        }, "LSET", logArgs);
    }
}
