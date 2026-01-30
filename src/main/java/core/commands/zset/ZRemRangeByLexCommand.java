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

public class ZRemRangeByLexCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 4) {
            client.sendError("usage: ZREMRANGEBYLEX key min max");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String min = new String(args.get(2), StandardCharsets.UTF_8);
        String max = new String(args.get(3), StandardCharsets.UTF_8);

        Object[] logArgs = new Object[]{key, min, max};
        
        client.executeWrite(() -> {
            ValueEntry v = Carade.db.get(client.getDbIndex(), key);
            if (v == null) {
                client.sendInteger(0);
                return;
            }
            if (v.type != DataType.ZSET) {
                throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            CaradeZSet zset = (CaradeZSet) v.getValue();
            
            ZNode minNode;
            boolean minInc = true;
            if (min.equals("-")) {
                minNode = new ZNode(Double.NEGATIVE_INFINITY, "");
            } else if (min.equals("+")) {
                minNode = new ZNode(Double.POSITIVE_INFINITY, "");
            } else {
                if (!min.startsWith("(") && !min.startsWith("[")) {
                     throw new RuntimeException("ERR min or max not valid string range item");
                }
                minInc = min.startsWith("[");
                minNode = new ZNode(0, min.substring(1));
            }
            
            ZNode maxNode;
            boolean maxInc = true;
            if (max.equals("+")) {
                maxNode = new ZNode(Double.POSITIVE_INFINITY, "");
            } else if (max.equals("-")) {
                maxNode = new ZNode(Double.NEGATIVE_INFINITY, "");
            } else {
                 if (!max.startsWith("(") && !max.startsWith("[")) {
                     throw new RuntimeException("ERR min or max not valid string range item");
                }
                maxInc = max.startsWith("[");
                maxNode = new ZNode(0, max.substring(1));
            }
            
            NavigableSet<ZNode> subset;
             if (minNode.score == Double.NEGATIVE_INFINITY && maxNode.score == Double.POSITIVE_INFINITY) {
                subset = zset.sorted;
            } else if (minNode.score == Double.NEGATIVE_INFINITY) {
                subset = zset.sorted.headSet(maxNode, maxInc);
            } else if (maxNode.score == Double.POSITIVE_INFINITY) {
                subset = zset.sorted.tailSet(minNode, minInc);
            } else {
                try {
                    subset = zset.sorted.subSet(minNode, minInc, maxNode, maxInc);
                } catch (IllegalArgumentException e) {
                    client.sendInteger(0);
                    return;
                }
            }
            
            List<String> toRemove = new ArrayList<>();
            for (ZNode node : subset) {
                toRemove.add(node.member);
            }
            
            for (String member : toRemove) {
                Double score = zset.scores.remove(member);
                if (score != null) {
                    zset.sorted.remove(new ZNode(score, member));
                }
            }
            
            if (zset.scores.isEmpty()) {
                Carade.db.remove(client.getDbIndex(), key);
            } else {
                v.touch();
                Carade.notifyWatchers(key);
            }
            
            client.sendInteger(toRemove.size());
            
        }, "ZREMRANGEBYLEX", logArgs);
    }
}
