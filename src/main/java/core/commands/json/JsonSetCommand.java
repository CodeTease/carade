package core.commands.json;

import core.commands.Command;
import core.db.DataType;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.network.ClientHandler;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.io.IOException;

public class JsonSetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'JSON.SET' command");
            return;
        }

        String key = new String(args.get(0));
        String path = new String(args.get(1));
        String jsonStr = new String(args.get(2));
        
        // Ensure path starts with $ or . or is valid
        if (!path.startsWith("$") && !path.startsWith(".")) {
             // Maybe allow it? but standard is $.
        }

        JsonNode newValue;
        try {
            newValue = JsonUtils.parse(jsonStr);
        } catch (IOException e) {
            client.sendError("ERR invalid JSON");
            return;
        }

        CaradeDatabase db = CaradeDatabase.getInstance();
        int dbIndex = client.getDbIndex();
        
        client.executeWrite(() -> {
            ValueEntry entry = db.get(dbIndex, key);
            
            if (entry == null) {
                // If key doesn't exist, we can only set if path is root
                if (path.equals(".") || path.equals("$")) {
                    db.put(dbIndex, key, new ValueEntry(newValue, DataType.JSON, -1));
                    client.sendSimpleString("OK");
                } else {
                    client.sendError("ERR new objects must be created at the root");
                }
            } else {
                if (entry.type != DataType.JSON) {
                    client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
                    return;
                }
                
                JsonNode currentRoot = (JsonNode) entry.getValue();
                JsonNode updatedRoot = JsonUtils.setByPath(currentRoot, path, newValue);
                entry.setValue(updatedRoot);
                client.sendSimpleString("OK");
            }
        }, "JSON.SET", key, path, jsonStr);
    }
}
