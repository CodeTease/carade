package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.List;

public class RPopLPushCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: RPOPLPUSH source destination");
            return;
        }

        String source = new String(args.get(1), StandardCharsets.UTF_8);
        String destination = new String(args.get(2), StandardCharsets.UTF_8);
        
        final String[] valRef = {null};
        try {
            client.executeWrite(() -> {
                ValueEntry entry = Carade.db.get(client.getDbIndex(), source);
                if (entry == null || entry.type != DataType.LIST) {
                    // Will handle send outside or allow null valRef
                } else {
                    ConcurrentLinkedDeque<String> srcList = (ConcurrentLinkedDeque<String>) entry.getValue();
                    String val = srcList.pollLast();
                    if (val != null) {
                        if (srcList.isEmpty()) Carade.db.remove(client.getDbIndex(), source);
                        Carade.notifyWatchers(source);
                        
                        Carade.db.getStore(client.getDbIndex()).compute(destination, (k, v) -> {
                            if (v == null) {
                                ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
                                list.addFirst(val);
                                return new ValueEntry(list, DataType.LIST, -1);
                            } else if (v.type == DataType.LIST) {
                                ((ConcurrentLinkedDeque<String>) v.getValue()).addFirst(val);
                                return v;
                            }
                            throw new RuntimeException("WRONGTYPE");
                        });
                        Carade.notifyWatchers(destination);
                        valRef[0] = val;
                    }
                }
            }, "RPOPLPUSH", source, destination);

            if (valRef[0] != null) {
                client.sendBulkString(valRef[0]);
            } else {
                ValueEntry e = Carade.db.get(client.getDbIndex(), source);
                if (e != null && e.type != DataType.LIST) client.sendError("WRONGTYPE");
                else client.sendNull();
            }
        } catch (RuntimeException e) {
            if ("WRONGTYPE".equals(e.getMessage())) client.sendError("WRONGTYPE");
            else throw e;
        }
    }
}
