package core.commands.string;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import core.server.WriteSequencer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SetCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: SET key value [EX seconds]");
            return;
        }

        Carade.performEvictionIfNeeded();
        long ttlVal = -1;
        boolean nx = false;
        boolean xx = false;
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        byte[] val = args.get(2);
        
        // Parse options
        for (int i = 3; i < args.size(); i++) {
            String arg = new String(args.get(i), StandardCharsets.UTF_8).toUpperCase();
            if (arg.equals("EX") && i + 1 < args.size()) {
                 try { 
                    ttlVal = Long.parseLong(new String(args.get(++i), StandardCharsets.UTF_8)) * 1000 + System.currentTimeMillis(); 
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
        
        final long finalTtl = ttlVal;

        if (nx && Carade.db.exists(client.dbIndex, key)) {
            client.sendResponse(Resp.bulkString((byte[])null), "(nil)");
            return;
        }
        if (xx && !Carade.db.exists(client.dbIndex, key)) {
            client.sendResponse(Resp.bulkString((byte[])null), "(nil)");
            return;
        }

        // Construct AOF args for correct logging
        List<byte[]> aofArgs = new ArrayList<>();
        aofArgs.add("SET".getBytes(StandardCharsets.UTF_8));
        aofArgs.add(key.getBytes(StandardCharsets.UTF_8));
        aofArgs.add(val);
        if (finalTtl != -1) {
            long seconds = (finalTtl - System.currentTimeMillis()) / 1000;
            if (seconds < 0) seconds = 1;
            aofArgs.add("EX".getBytes(StandardCharsets.UTF_8));
            aofArgs.add(String.valueOf(seconds).getBytes(StandardCharsets.UTF_8));
        }
        byte[] cmdBytes = Resp.array(aofArgs);

        WriteSequencer.getInstance().executeWrite(() -> {
            Carade.db.put(client.dbIndex, key, new ValueEntry(val, DataType.STRING, finalTtl != -1 ? finalTtl : -1));
            Carade.notifyWatchers(key);
        }, cmdBytes);
        
        client.sendResponse(Resp.simpleString("OK"), "OK");
    }
}
