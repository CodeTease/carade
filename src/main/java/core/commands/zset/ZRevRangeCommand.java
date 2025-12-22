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

public class ZRevRangeCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: ZREVRANGE key start stop [WITHSCORES]");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        boolean withScores = args.size() > 4 && new String(args.get(args.size()-1), StandardCharsets.UTF_8).equalsIgnoreCase("WITHSCORES");
        
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        if (entry == null || entry.type != DataType.ZSET) {
             if (entry != null && entry.type != DataType.ZSET) client.sendError("WRONGTYPE");
             else client.sendArray(Collections.emptyList());
        } else {
            try {
                int start = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
                int end = Integer.parseInt(new String(args.get(3), StandardCharsets.UTF_8));
                CaradeZSet zset = (CaradeZSet) entry.getValue();
                int size = zset.size();
                
                if (start < 0) start += size;
                if (end < 0) end += size;
                if (start < 0) start = 0;
                
                List<byte[]> result = new ArrayList<>();
                List<String> resultStr = new ArrayList<>();
                if (start <= end) {
                    Iterator<ZNode> it = zset.sorted.descendingIterator();
                    int idx = 0;
                    while (it.hasNext() && idx <= end) {
                        ZNode node = it.next();
                        if (idx >= start) {
                            result.add(node.member.getBytes(StandardCharsets.UTF_8));
                            resultStr.add(node.member);
                            if (withScores) {
                                String s = String.valueOf(node.score);
                                if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                                result.add(s.getBytes(StandardCharsets.UTF_8));
                                resultStr.add(s);
                            }
                        }
                        idx++;
                    }
                }
                
                if (client.isResp()) client.send(true, Resp.array(result), null);
                else {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < resultStr.size(); i++) {
                        sb.append((i+1) + ") \"" + resultStr.get(i) + "\"\n");
                    }
                    client.send(false, null, sb.toString().trim());
                }
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not an integer or out of range");
            }
        }
    }
}
