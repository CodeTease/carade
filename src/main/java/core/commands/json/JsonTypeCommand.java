package core.commands.json;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class JsonTypeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'JSON.TYPE' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String path = args.size() > 2 ? new String(args.get(2), StandardCharsets.UTF_8) : "$";

        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);

        if (entry == null) {
            client.sendNull();
            return;
        }

        if (entry.type != DataType.JSON) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }

        JsonNode root = (JsonNode) entry.getValue();
        JsonNode target = JsonUtils.getByPath(root, path);

        if (target == null) {
            client.sendNull();
        } else {
            String type = "unknown";
            if (target.isObject()) type = "object";
            else if (target.isArray()) type = "array";
            else if (target.isTextual()) type = "string";
            else if (target.isIntegralNumber()) type = "integer";
            else if (target.isFloatingPointNumber()) type = "number";
            else if (target.isBoolean()) type = "boolean";
            else if (target.isNull()) type = "null";
            
            client.sendBulkString(type);
        }
    }
}
