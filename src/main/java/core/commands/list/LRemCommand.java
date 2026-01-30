package core.commands.list;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LRemCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("wrong number of arguments for 'lrem' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        final int[] removedCount = {0};

        try {
            int count = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
            String element = new String(args.get(3), StandardCharsets.UTF_8);

            client.executeWrite(() -> {
                Carade.db.getStore(client.dbIndex).computeIfPresent(key, (k, v) -> {
                    if (v.type != DataType.LIST) return v;

                    ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
                    int toRemove = count;
                    int removed = 0;
                    
                    if (toRemove == 0) {
                        // Remove all occurrences
                        while (list.removeFirstOccurrence(element)) {
                            removed++;
                        }
                    } else if (toRemove > 0) {
                        Iterator<String> it = list.iterator();
                        while (it.hasNext() && removed < toRemove) {
                            if (it.next().equals(element)) {
                                it.remove();
                                removed++;
                            }
                        }
                    } else {
                        // Remove first |count| occurrences from tail
                        toRemove = -toRemove;
                        Iterator<String> it = list.descendingIterator();
                        while (it.hasNext() && removed < toRemove) {
                            if (it.next().equals(element)) {
                                it.remove();
                                removed++;
                            }
                        }
                    }
                    
                    removedCount[0] = removed;
                    
                    if (list.isEmpty()) return null;
                    return v;
                });
                if (removedCount[0] > 0) Carade.notifyWatchers(key);
            }, "LREM", key, String.valueOf(count), element);

            ValueEntry entry = Carade.db.get(client.dbIndex, key);
            if (entry != null && entry.type != DataType.LIST) {
                client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            } else {
                client.sendInteger(removedCount[0]);
            }
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
