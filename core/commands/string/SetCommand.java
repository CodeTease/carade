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
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        byte[] val = args.get(2);
        
        if (args.size() >= 5 && new String(args.get(3), StandardCharsets.UTF_8).equalsIgnoreCase("EX")) {
            try { 
                ttl = Long.parseLong(new String(args.get(4), StandardCharsets.UTF_8)); 
            } catch (Exception e) {}
        }
        
        Carade.db.put(key, new ValueEntry(val, DataType.STRING, ttl));
        Carade.notifyWatchers(key);
        
        if (ttl > 0) Carade.aofHandler.log("SET", key, val, "EX", String.valueOf(ttl));
        else Carade.aofHandler.log("SET", key, val);
        
        client.sendResponse(Resp.simpleString("OK"), "OK");
    }
}
