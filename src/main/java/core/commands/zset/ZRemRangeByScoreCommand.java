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
import java.util.List;
import java.util.NavigableSet;

public class ZRemRangeByScoreCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("wrong number of arguments for 'zremrangebyscore' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        final int[] removedCount = {0};

        try {
            String minStr = new String(args.get(2), StandardCharsets.UTF_8).toLowerCase();
            String maxStr = new String(args.get(3), StandardCharsets.UTF_8).toLowerCase();

            boolean minExclusive = minStr.startsWith("(");
            if (minExclusive) minStr = minStr.substring(1);
            final double min = minStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (minStr.equals("+inf") || minStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(minStr));

            boolean maxExclusive = maxStr.startsWith("(");
            if (maxExclusive) maxStr = maxStr.substring(1);
            final double max = maxStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (maxStr.equals("+inf") || maxStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(maxStr));
            
            final boolean finalMinExclusive = minExclusive;
            final boolean finalMaxExclusive = maxExclusive;

            client.executeWrite(() -> {
                Carade.db.getStore(client.dbIndex).computeIfPresent(key, (k, v) -> {
                    if (v.type != DataType.ZSET) return v; // Throwing exception here would be caught
                    
                    CaradeZSet zset = (CaradeZSet) v.getValue();
                    NavigableSet<ZNode> subset = zset.rangeByScore(min, !finalMinExclusive, max, !finalMaxExclusive);
                    
                    // Avoid concurrent modification if we remove from the set we are iterating?
                    // sorted.subSet returns a view. Removing from view removes from backing set.
                    // But we also need to remove from `scores` map.
                    
                    // Better to collect items to remove first.
                    List<ZNode> toRemove = new ArrayList<>(subset);
                    removedCount[0] = toRemove.size();
                    
                    for (ZNode node : toRemove) {
                        zset.scores.remove(node.member);
                        zset.sorted.remove(node);
                    }
                    
                    if (zset.size() == 0) return null;
                    return v;
                });
                if (removedCount[0] > 0) Carade.notifyWatchers(key);
            }, "ZREMRANGEBYSCORE", key, args.get(2), args.get(3));

            // Check if it was WRONGTYPE
            ValueEntry entry = Carade.db.get(client.dbIndex, key);
            if (entry != null && entry.type != DataType.ZSET) {
                client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            } else {
                client.sendInteger(removedCount[0]);
            }

        } catch (NumberFormatException e) {
            client.sendError("ERR min or max is not a float");
        }
    }
}
