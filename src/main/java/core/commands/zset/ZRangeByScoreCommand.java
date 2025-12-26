package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;

public class ZRangeByScoreCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        String cmd = new String(args.get(0), StandardCharsets.UTF_8).toUpperCase();
        if (args.size() < 4) {
            client.sendError("usage: " + cmd + " key min max [WITHSCORES] [LIMIT offset count]");
            return;
        }
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        if (entry == null || entry.type != DataType.ZSET) {
            if (entry != null && entry.type != DataType.ZSET) {
                client.sendError("WRONGTYPE");
            }
            else {
                client.sendArray(Collections.emptyList());
            }
        } else {
            try {
                // Parse min/max based on command
                String minStr, maxStr;
                if (cmd.equals("ZRANGEBYSCORE")) {
                    minStr = new String(args.get(2), StandardCharsets.UTF_8).toLowerCase();
                    maxStr = new String(args.get(3), StandardCharsets.UTF_8).toLowerCase();
                } else {
                    maxStr = new String(args.get(2), StandardCharsets.UTF_8).toLowerCase();
                    minStr = new String(args.get(3), StandardCharsets.UTF_8).toLowerCase();
                }
                
                boolean minExclusive = minStr.startsWith("(");
                if (minExclusive) minStr = minStr.substring(1);
                double min = minStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (minStr.equals("+inf") || minStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(minStr));
                
                boolean maxExclusive = maxStr.startsWith("(");
                if (maxExclusive) maxStr = maxStr.substring(1);
                double max = maxStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (maxStr.equals("+inf") || maxStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(maxStr));
                
                // Parse Options
                boolean withScores = false;
                int offset = 0;
                int count = Integer.MAX_VALUE;
                
                for (int i = 4; i < args.size(); i++) {
                    String arg = new String(args.get(i), StandardCharsets.UTF_8).toUpperCase();
                    if (arg.equals("WITHSCORES")) {
                        withScores = true;
                    } else if (arg.equals("LIMIT") && i + 2 < args.size()) {
                        offset = Integer.parseInt(new String(args.get(++i), StandardCharsets.UTF_8));
                        count = Integer.parseInt(new String(args.get(++i), StandardCharsets.UTF_8));
                    }
                }
                
                CaradeZSet zset = (CaradeZSet) entry.getValue();
                NavigableSet<ZNode> subset = zset.rangeByScore(min, !minExclusive, max, !maxExclusive);
                
                Iterator<ZNode> it = cmd.equals("ZRANGEBYSCORE") ? subset.iterator() : subset.descendingIterator();
                
                // Apply LIMIT
                int skipped = 0;
                while (skipped < offset && it.hasNext()) {
                    it.next();
                    skipped++;
                }
                
                List<byte[]> result = new ArrayList<>();
                List<String> resultStr = new ArrayList<>();
                int added = 0;
                while (it.hasNext() && added < count) {
                    ZNode node = it.next();
                    result.add(node.member.getBytes(StandardCharsets.UTF_8));
                    resultStr.add(node.member);
                    if (withScores) {
                        String s = String.valueOf(node.score);
                        if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                        result.add(s.getBytes(StandardCharsets.UTF_8));
                        resultStr.add(s);
                    }
                    added++;
                }
                
                if (client.isResp()) {
                    client.send(true, Resp.array(result), null);
                } else {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < resultStr.size(); i++) {
                        sb.append((i+1) + ") \"" + resultStr.get(i) + "\"\n");
                    }
                    client.send(false, null, sb.toString().trim());
                }
            } catch (NumberFormatException e) {
                client.sendError("ERR min or max is not a float");
            }
        }
    }
}
