package core.commands.set;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SInterCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: SINTER key [key ...]");
            return;
        }

        String firstKey = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), firstKey);
        
        if (entry == null || entry.type != DataType.SET) {
             client.sendArray(Collections.emptyList());
             return;
        }
        
        Set<String> result = new HashSet<>((Set<String>) entry.getValue());
        for (int i = 2; i < args.size(); i++) {
             ValueEntry e = Carade.db.get(client.getDbIndex(), new String(args.get(i), StandardCharsets.UTF_8));
             if (e == null || e.type != DataType.SET) {
                 result.clear();
                 break;
             }
             result.retainAll((Set<String>) e.getValue());
        }
        
        List<byte[]> resp = new ArrayList<>();
        for(String s : result) resp.add(s.getBytes(StandardCharsets.UTF_8));
        client.sendArray(resp);
    }
}
