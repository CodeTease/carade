package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.List;
import java.util.Arrays;

public class LMoveCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 5) {
            client.sendError("usage: LMOVE source destination LEFT|RIGHT LEFT|RIGHT");
            return;
        }

        String source = new String(args.get(1), StandardCharsets.UTF_8);
        String destination = new String(args.get(2), StandardCharsets.UTF_8);
        String whereFrom = new String(args.get(3), StandardCharsets.UTF_8).toUpperCase();
        String whereTo = new String(args.get(4), StandardCharsets.UTF_8).toUpperCase();
        
        if (!Arrays.asList("LEFT", "RIGHT").contains(whereFrom) || !Arrays.asList("LEFT", "RIGHT").contains(whereTo)) {
            client.sendError("ERR syntax error");
            return;
        }
        
        final String[] valRef = {null};
        try {
            client.executeWrite(() -> {
                ValueEntry entry = Carade.db.get(client.getDbIndex(), source);
                if (entry != null && entry.type == DataType.LIST) {
                    ConcurrentLinkedDeque<String> srcList = (ConcurrentLinkedDeque<String>) entry.getValue();
                    String val = whereFrom.equals("RIGHT") ? srcList.pollLast() : srcList.pollFirst();
                    if (val != null) {
                        if (srcList.isEmpty()) Carade.db.remove(client.getDbIndex(), source);
                        Carade.notifyWatchers(source);
                        
                        Carade.db.getStore(client.getDbIndex()).compute(destination, (k, v) -> {
                            if (v == null) {
                                ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
                                if (whereTo.equals("LEFT")) list.addFirst(val); else list.addLast(val);
                                return new ValueEntry(list, DataType.LIST, -1);
                            } else if (v.type == DataType.LIST) {
                                ConcurrentLinkedDeque<String> l = (ConcurrentLinkedDeque<String>) v.getValue();
                                if (whereTo.equals("LEFT")) l.addFirst(val); else l.addLast(val);
                                return v;
                            }
                            throw new RuntimeException("WRONGTYPE");
                        });
                        Carade.notifyWatchers(destination);
                        valRef[0] = val;
                    }
                }
            }, "LMOVE", source, destination, whereFrom, whereTo);

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
