package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ZRemRangeByRankCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("wrong number of arguments for 'zremrangebyrank' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        final int[] removedCount = {0};

        try {
            int start = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
            int stop = Integer.parseInt(new String(args.get(3), StandardCharsets.UTF_8));
            
            client.executeWrite(() -> {
                Carade.db.getStore(client.dbIndex).computeIfPresent(key, (k, v) -> {
                    if (v.type != DataType.ZSET) return v;

                    CaradeZSet zset = (CaradeZSet) v.getValue();
                    int size = zset.size();
                    int s = start;
                    int e = stop;

                    if (s < 0) s += size;
                    if (e < 0) e += size;
                    if (s < 0) s = 0;
                    
                    if (s > e || s >= size) return v; // Nothing to remove

                    if (e >= size) e = size - 1;
                    
                    // Identify nodes to remove
                    List<ZNode> toRemove = new ArrayList<>();
                    Iterator<ZNode> it = zset.sorted.iterator();
                    int idx = 0;
                    while (it.hasNext()) {
                        ZNode node = it.next();
                        if (idx >= s && idx <= e) {
                            toRemove.add(node);
                        }
                        if (idx > e) break;
                        idx++;
                    }
                    
                    removedCount[0] = toRemove.size();
                    for (ZNode node : toRemove) {
                        zset.scores.remove(node.member);
                        zset.sorted.remove(node);
                    }
                    
                    if (zset.size() == 0) return null;
                    return v;
                });
                if (removedCount[0] > 0) Carade.notifyWatchers(key);
            }, "ZREMRANGEBYRANK", key, String.valueOf(start), String.valueOf(stop));

            ValueEntry entry = Carade.db.get(client.dbIndex, key);
            if (entry != null && entry.type != DataType.ZSET) {
                client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            } else {
                client.sendInteger(removedCount[0]);
            }
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
        }
    }
}
