package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LInsertCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 5) {
            client.sendError("usage: LINSERT key BEFORE|AFTER pivot element");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String position = new String(args.get(2), StandardCharsets.UTF_8).toUpperCase();
        String pivot = new String(args.get(3), StandardCharsets.UTF_8);
        String element = new String(args.get(4), StandardCharsets.UTF_8);

        if (!position.equals("BEFORE") && !position.equals("AFTER")) {
            client.sendError("ERR syntax error");
            return;
        }

        Object[] logArgs = new Object[]{key, position, pivot, element};

        client.executeWrite(() -> {
            ValueEntry v = Carade.db.get(client.getDbIndex(), key);
            if (v == null) {
                client.sendInteger(0);
                return;
            }
            if (v.type != DataType.LIST) {
                throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
            if (list.isEmpty()) {
                client.sendInteger(0);
                return;
            }
            
            boolean found = false;
            ConcurrentLinkedDeque<String> newList = new ConcurrentLinkedDeque<>();
            for (String s : list) {
                if (!found && s.equals(pivot)) {
                    found = true;
                    if (position.equals("BEFORE")) {
                        newList.add(element);
                        newList.add(s);
                    } else {
                        newList.add(s);
                        newList.add(element);
                    }
                } else {
                    newList.add(s);
                }
            }

            if (!found) {
                client.sendInteger(-1);
                return;
            }

            v.setValue(newList);
            v.touch();
            Carade.notifyWatchers(key);
            client.sendInteger(newList.size());

        }, "LINSERT", logArgs);
    }
}
