package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: SET key value [EX seconds]");
            return;
        }

        Carade.performEvictionIfNeeded();
        long ttl = -1;
        boolean nx = false;
        boolean xx = false;
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        byte[] val = args.get(2);
        
        // Parse options
        for (int i = 3; i < args.size(); i++) {
            String arg = new String(args.get(i), StandardCharsets.UTF_8).toUpperCase();
            if (arg.equals("EX") && i + 1 < args.size()) {
                 try { 
                    ttl = Long.parseLong(new String(args.get(++i), StandardCharsets.UTF_8)) * 1000 + System.currentTimeMillis(); 
                } catch (Exception e) {
                    client.sendError("ERR value is not an integer or out of range");
                    return;
                }
            } else if (arg.equals("NX")) {
                nx = true;
            } else if (arg.equals("XX")) {
                xx = true;
            }
        }
        
        if (nx && Carade.db.exists(key)) {
            client.sendResponse(Resp.bulkString((byte[])null), "(nil)");
            return;
        }
        if (xx && !Carade.db.exists(key)) {
            client.sendResponse(Resp.bulkString((byte[])null), "(nil)");
            return;
        }

        Carade.db.put(key, new ValueEntry(val, DataType.STRING, ttl != -1 ? ttl : -1));
        Carade.notifyWatchers(key);
        
        // AOF Logging
        if (ttl != -1) {
            long seconds = (ttl - System.currentTimeMillis()) / 1000;
            if (seconds < 0) seconds = 1; // Minimum 1s if just set
            Carade.aofHandler.log("SET", key, val, "EX", String.valueOf(seconds));
        } else {
            Carade.aofHandler.log("SET", key, val);
        }
        
        client.sendResponse(Resp.simpleString("OK"), "OK");
    }
}
