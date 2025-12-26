package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NavigableSet;

public class ZLexCountCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() != 4) {
            client.sendError("usage: ZLEXCOUNT key min max");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String min = new String(args.get(2), StandardCharsets.UTF_8);
        String max = new String(args.get(3), StandardCharsets.UTF_8);

        ValueEntry v = Carade.db.get(client.getDbIndex(), key);
        if (v == null) {
            client.sendInteger(0);
            return;
        }
        if (v.type != DataType.ZSET) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }

        CaradeZSet zset = (CaradeZSet) v.getValue();

        try {
            ZNode minNode;
            boolean minInc = true;
            if (min.equals("-")) {
                minNode = new ZNode(Double.NEGATIVE_INFINITY, "");
            } else if (min.equals("+")) {
                minNode = new ZNode(Double.POSITIVE_INFINITY, "");
            } else {
                if (!min.startsWith("(") && !min.startsWith("[")) {
                     client.sendError("ERR min or max not valid string range item");
                     return;
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
                     client.sendError("ERR min or max not valid string range item");
                     return;
                }
                maxInc = max.startsWith("[");
                maxNode = new ZNode(0, max.substring(1));
            }
            
            // For ZSet sorted set, we can rely on subSet ONLY IF score is same.
            // If score is different, ZNode comparison uses score.
            // So a node with score 1 is > node with score 0 regardless of string.
            // If user uses ZLEXCOUNT on set with different scores, it might return 0 or garbage.
            // Redis assumes score is same.
            // But we must construct the boundary nodes with the SAME score as elements (0).
            // We used 0 above.
            
            // However, - and + need special handling.
            // If min is "-", we want from head.
            // If max is "+", we want to tail.
            
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
                    // Start > End
                    client.sendInteger(0);
                    return;
                }
            }
            
            client.sendInteger(subset.size());
            
        } catch (IllegalArgumentException e) {
            client.sendError("ERR " + e.getMessage());
        }
    }
}
