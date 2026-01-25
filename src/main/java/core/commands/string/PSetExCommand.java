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

public class PSetExCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 4) {
            client.sendError("usage: PSETEX key milliseconds value");
            return;
        }

        Carade.performEvictionIfNeeded();
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        long ms;
        try {
            ms = Long.parseLong(new String(args.get(2), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }
        
        if (ms <= 0) {
            client.sendError("ERR invalid expire time in PSETEX");
            return;
        }

        byte[] val = args.get(3);
        long expireAt = System.currentTimeMillis() + ms;
        
        List<byte[]> aofArgs = new ArrayList<>();
        aofArgs.add("PSETEX".getBytes(StandardCharsets.UTF_8));
        aofArgs.add(key.getBytes(StandardCharsets.UTF_8));
        aofArgs.add(String.valueOf(ms).getBytes(StandardCharsets.UTF_8));
        aofArgs.add(val);
        
        byte[] cmdBytes = Resp.array(aofArgs);

        WriteSequencer.getInstance().executeWrite(() -> {
            Carade.db.put(client.getDbIndex(), key, new ValueEntry(val, DataType.STRING, expireAt));
            Carade.notifyWatchers(key);
        }, cmdBytes);
        
        client.sendResponse(Resp.simpleString("OK"), "OK");
    }
}
