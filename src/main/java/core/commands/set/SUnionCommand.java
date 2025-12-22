package core.commands.set;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SUnionCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: SUNION key [key ...]");
            return;
        }

        Set<String> result = new HashSet<>();
        for (int i = 1; i < args.size(); i++) {
             ValueEntry e = Carade.db.get(client.getDbIndex(), new String(args.get(i), StandardCharsets.UTF_8));
             if (e != null && e.type == DataType.SET) {
                 result.addAll((Set<String>) e.getValue());
             }
        }
        
        List<byte[]> resp = new ArrayList<>();
        for(String s : result) resp.add(s.getBytes(StandardCharsets.UTF_8));
        client.sendArray(resp);
    }
}
