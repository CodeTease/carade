package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LPosCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: LPOS key element");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String element = new String(args.get(2), StandardCharsets.UTF_8);
        
        ValueEntry v = Carade.db.get(client.getDbIndex(), key);
        if (v == null) {
            client.sendNull();
            return;
        }
        if (v.type != DataType.LIST) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        
        ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
        int index = 0;
        boolean found = false;
        for (String s : list) {
            if (s.equals(element)) {
                found = true;
                break;
            }
            index++;
        }
        
        if (found) client.sendInteger(index);
        else client.sendNull();
    }
}
