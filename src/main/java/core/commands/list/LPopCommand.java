package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LPopCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: LPOP key");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);
        if (entry == null) {
            client.sendResponse(Resp.bulkString((byte[])null), "(nil)");
        } else if (entry.type != DataType.LIST) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
        } else {
            ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.getValue();
            if (list.isEmpty()) {
                client.sendResponse(Resp.bulkString((byte[])null), "(nil)");
            } else {
                final String[] valRef = {null};
                client.executeWrite(() -> {
                    ValueEntry e = Carade.db.get(client.dbIndex, key);
                    if (e != null && e.type == DataType.LIST) {
                        ConcurrentLinkedDeque<String> l = (ConcurrentLinkedDeque<String>) e.getValue();
                        valRef[0] = l.pollFirst();
                        if (valRef[0] != null) {
                            if (l.isEmpty()) Carade.db.remove(client.dbIndex, key);
                            Carade.notifyWatchers(key);
                        }
                    }
                }, "LPOP", key);

                String val = valRef[0];
                if (val != null) {
                    client.sendResponse(Resp.bulkString(val.getBytes(StandardCharsets.UTF_8)), val);
                } else {
                    client.sendResponse(Resp.bulkString((byte[])null), "(nil)");
                }
            }
        }
    }
}
