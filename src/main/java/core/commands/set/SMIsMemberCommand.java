package core.commands.set;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SMIsMemberCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'smismember' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry v = Carade.db.get(client.getDbIndex(), key);

        List<Object> result = new ArrayList<>();
        
        if (v == null) {
            for (int i = 2; i < args.size(); i++) {
                result.add(0L);
            }
        } else if (v.type != DataType.SET) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        } else {
            Set<String> set = (Set<String>) v.getValue();
            for (int i = 2; i < args.size(); i++) {
                String member = new String(args.get(i), StandardCharsets.UTF_8);
                result.add(set.contains(member) ? 1L : 0L);
            }
        }
        
        client.sendMixedArray(result);
    }
}
