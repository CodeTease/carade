package core.commands.server;

import core.Carade;
import core.commands.Command;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MemoryUsageCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("ERR wrong number of arguments for 'memory usage' command");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry v = Carade.db.get(client.getDbIndex(), key);
        if (v == null) {
            client.sendNull();
            return;
        }
        
        // Very rough estimation
        // ValueEntry overhead (approx 48 bytes) + Key (string len) + Value (payload)
        // This is a naive implementation as requested.
        long size = 48 + key.length();
        
        // Estimate value size
        if (v.getValue() instanceof byte[]) {
            size += ((byte[])v.getValue()).length + 16; // byte array overhead
        } else if (v.getValue() instanceof String) {
            size += ((String)v.getValue()).length() * 2 + 32;
        } else if (v.getValue() instanceof java.util.Collection) {
            size += ((java.util.Collection<?>)v.getValue()).size() * 64; // Approx 64 bytes per entry
        } else if (v.getValue() instanceof java.util.Map) {
            size += ((java.util.Map<?,?>)v.getValue()).size() * 96; // Approx 96 bytes per entry
        } else if (v.getValue() instanceof core.structs.CaradeZSet) {
             size += ((core.structs.CaradeZSet)v.getValue()).size() * 128;
        }
        
        // Samples option is ignored as per task spec (or simple impl)
        client.sendInteger(size);
    }
}
