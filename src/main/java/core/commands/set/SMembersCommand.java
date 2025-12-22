package core.commands.set;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SMembersCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: SMEMBERS key");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        
        if (entry == null || entry.type != DataType.SET) {
            client.sendArray(Collections.emptyList());
        } else {
            Set<String> set = (Set<String>) entry.getValue();
            List<byte[]> list = new ArrayList<>();
            for(String s : set) {
                list.add(s.getBytes(StandardCharsets.UTF_8));
            }
            client.sendArray(list);
        }
    }
}
