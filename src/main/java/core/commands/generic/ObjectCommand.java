package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ObjectCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: OBJECT <subcommand> <key> [hints]");
            return;
        }

        String sub = new String(args.get(1), StandardCharsets.UTF_8).toUpperCase();
        String key = new String(args.get(2), StandardCharsets.UTF_8);
        ValueEntry v = Carade.db.get(client.getDbIndex(), key);

        if (v == null) {
            client.sendNull();
            return;
        }

        switch (sub) {
            case "ENCODING":
                // In Carade, we don't have complex encodings (e.g., ziplist vs linkedlist) visible mostly.
                // But we can map DataType to standard Redis encoding names for compatibility.
                String enc = "raw";
                if (v.type == DataType.STRING) {
                    try {
                        Long.parseLong(new String((byte[])v.getValue()));
                        enc = "int";
                    } catch (Exception e) { enc = "raw"; }
                } else if (v.type == DataType.LIST) enc = "quicklist"; // Pretend
                else if (v.type == DataType.SET) enc = "hashtable";
                else if (v.type == DataType.HASH) enc = "hashtable";
                else if (v.type == DataType.ZSET) enc = "skiplist";
                client.sendBulkString(enc);
                break;
            case "REFCOUNT":
                client.sendInteger(1); // No refcount impl
                break;
            case "IDLETIME":
                long idle = (System.currentTimeMillis() - v.lastAccessed) / 1000;
                client.sendInteger(idle);
                break;
            case "FREQ":
                // LFU freq
                client.sendInteger(v.frequency);
                break;
            default:
                client.sendError("ERR unknown subcommand for 'object'");
        }
    }
}
