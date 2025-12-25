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

public class ZRevRangeByLexCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // ZREVRANGEBYLEX key max min [LIMIT offset count]
        if (args.size() < 4) {
            client.sendError("usage: ZREVRANGEBYLEX key max min [LIMIT offset count]");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String max = new String(args.get(2), StandardCharsets.UTF_8);
        String min = new String(args.get(3), StandardCharsets.UTF_8);
        
        int offset = 0;
        int count = Integer.MAX_VALUE;
        
        if (args.size() > 4) {
            if (args.size() != 7 || !new String(args.get(4), StandardCharsets.UTF_8).equalsIgnoreCase("LIMIT")) {
                client.sendError("ERR syntax error");
                return;
            }
            try {
                offset = Integer.parseInt(new String(args.get(5), StandardCharsets.UTF_8));
                count = Integer.parseInt(new String(args.get(6), StandardCharsets.UTF_8));
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not an integer or out of range");
                return;
            }
        }

        ValueEntry v = Carade.db.get(client.getDbIndex(), key);
        if (v == null) {
            client.sendArray(new ArrayList<>());
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
                    client.sendArray(new ArrayList<>());
                    return;
                }
            }
            
            // Reverse iteration
            NavigableSet<ZNode> revSubset = subset.descendingSet();
            
            List<byte[]> result = new ArrayList<>();
            int current = 0;
            int collected = 0;
            for (ZNode node : revSubset) {
                if (current >= offset) {
                    result.add(node.member.getBytes(StandardCharsets.UTF_8));
                    collected++;
                    if (collected >= count) break;
                }
                current++;
            }
            
            client.sendArray(result);
            
        } catch (Exception e) {
            client.sendError("ERR " + e.getMessage());
        }
    }
}
