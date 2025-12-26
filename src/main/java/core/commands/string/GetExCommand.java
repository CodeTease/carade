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

public class GetExCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: GETEX key [EX seconds|PX milliseconds|PERSIST]");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        
        // Parse options
        long expireAt = -2; // -2: no change, -1: persist
        boolean persist = false;
        
        for (int i = 2; i < args.size(); i++) {
            String opt = new String(args.get(i), StandardCharsets.UTF_8).toUpperCase();
            if (opt.equals("EX") && i + 1 < args.size()) {
                try {
                    long sec = Long.parseLong(new String(args.get(++i), StandardCharsets.UTF_8));
                    expireAt = System.currentTimeMillis() + (sec * 1000);
                } catch (NumberFormatException e) {
                    client.sendError("ERR value is not an integer or out of range");
                    return;
                }
            } else if (opt.equals("PX") && i + 1 < args.size()) {
                 try {
                    long ms = Long.parseLong(new String(args.get(++i), StandardCharsets.UTF_8));
                    expireAt = System.currentTimeMillis() + ms;
                } catch (NumberFormatException e) {
                    client.sendError("ERR value is not an integer or out of range");
                    return;
                }
            } else if (opt.equals("PERSIST")) {
                persist = true;
                expireAt = -1;
            } else {
                 client.sendError("ERR syntax error");
                 return;
            }
        }
        
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        if (entry == null) {
            client.sendNull();
            return;
        }
        
        if (entry.type != DataType.STRING) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }
        
        byte[] val = (byte[]) entry.getValue();
        
        // If no options provided, behave like GET
        if (expireAt == -2 && !persist) {
            client.sendResponse(Resp.bulkString(val), new String(val, StandardCharsets.UTF_8));
            return;
        }
        
        final long finalExpireAt = expireAt;
        
        // Prepare AOF command
        List<byte[]> aofArgs = new ArrayList<>();
        if (persist) {
            aofArgs.add("PERSIST".getBytes(StandardCharsets.UTF_8));
            aofArgs.add(key.getBytes(StandardCharsets.UTF_8));
        } else {
            aofArgs.add("PEXPIREAT".getBytes(StandardCharsets.UTF_8));
            aofArgs.add(key.getBytes(StandardCharsets.UTF_8));
            aofArgs.add(String.valueOf(finalExpireAt).getBytes(StandardCharsets.UTF_8));
        }
        byte[] cmdBytes = Resp.array(aofArgs);

        WriteSequencer.getInstance().executeWrite(() -> {
             ValueEntry v = Carade.db.get(client.getDbIndex(), key);
             if (v != null) {
                 v.setExpireAt(finalExpireAt);
             }
        }, cmdBytes);
        
        client.sendResponse(Resp.bulkString(val), new String(val, StandardCharsets.UTF_8));
    }
}
