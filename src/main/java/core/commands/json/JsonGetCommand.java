package core.commands.json;

import core.commands.Command;
import core.db.DataType;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.network.ClientHandler;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public class JsonGetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 1) {
            client.sendError("ERR wrong number of arguments for 'JSON.GET' command");
            return;
        }

        String key = new String(args.get(0));
        String path = args.size() > 1 ? new String(args.get(1)) : "$";

        CaradeDatabase db = CaradeDatabase.getInstance();
        int dbIndex = client.getDbIndex();
        ValueEntry entry = db.get(dbIndex, key);

        if (entry == null) {
            client.sendNull();
            return;
        }

        if (entry.type != DataType.JSON) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }

        JsonNode root = (JsonNode) entry.getValue();
        JsonNode result = JsonUtils.getByPath(root, path);

        if (result == null) {
            client.sendNull();
        } else {
            client.sendBulkString(JsonUtils.stringify(result));
        }
    }
}
