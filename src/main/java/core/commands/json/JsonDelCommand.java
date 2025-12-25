package core.commands.json;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class JsonDelCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'JSON.DEL' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String path = args.size() > 2 ? new String(args.get(2), StandardCharsets.UTF_8) : "$";
        
        final long[] result = {0};

        try {
            client.executeWrite(() -> {
                 ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
                 if (entry == null) {
                     result[0] = 0;
                     return;
                 }
                 if (entry.type != DataType.JSON) {
                     throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                 }
                 
                 if (path.equals("$") || path.equals(".")) {
                     Carade.db.remove(client.getDbIndex(), key);
                     result[0] = 1;
                     Carade.notifyWatchers(key);
                 } else {
                     JsonNode root = (JsonNode) entry.getValue();
                     long deleted = JsonUtils.deleteByPath(root, path);
                     if (deleted > 0) {
                         entry.touch();
                         Carade.notifyWatchers(key);
                     }
                     result[0] = deleted;
                 }
            }, "JSON.DEL", key, path);
            
            client.sendInteger(result[0]);
        } catch (RuntimeException e) {
            client.sendError(e.getMessage());
        }
    }
}
