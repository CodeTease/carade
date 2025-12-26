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
import java.util.Map;
import java.util.HashMap;

public class ZUnionCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]");
            return;
        }

        int numKeys;
        try {
            numKeys = Integer.parseInt(new String(args.get(1), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            client.sendError("ERR value is not an integer or out of range");
            return;
        }

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            if (2 + i >= args.size()) {
                client.sendError("ERR syntax error");
                return;
            }
            keys.add(new String(args.get(2 + i), StandardCharsets.UTF_8));
        }

        // Parse options
        int currentArg = 2 + numKeys;
        List<Double> weights = new ArrayList<>();
        String aggregate = "SUM";
        boolean withScores = false;

        while (currentArg < args.size()) {
            String arg = new String(args.get(currentArg), StandardCharsets.UTF_8).toUpperCase();
            if (arg.equals("WEIGHTS")) {
                currentArg++;
                for (int i = 0; i < numKeys; i++) {
                    if (currentArg >= args.size()) {
                        client.sendError("ERR syntax error");
                        return;
                    }
                    try {
                        weights.add(Double.parseDouble(new String(args.get(currentArg), StandardCharsets.UTF_8)));
                    } catch (NumberFormatException e) {
                        client.sendError("ERR weight value is not a float");
                        return;
                    }
                    currentArg++;
                }
            } else if (arg.equals("AGGREGATE")) {
                currentArg++;
                if (currentArg >= args.size()) {
                    client.sendError("ERR syntax error");
                    return;
                }
                aggregate = new String(args.get(currentArg), StandardCharsets.UTF_8).toUpperCase();
                currentArg++;
            } else if (arg.equals("WITHSCORES")) {
                withScores = true;
                currentArg++;
            } else {
                client.sendError("ERR syntax error");
                return;
            }
        }
        
        if (weights.isEmpty()) {
            for (int i=0; i<numKeys; i++) weights.add(1.0);
        }

        Map<String, Double> finalMap = new HashMap<>();
        String finalAggregate = aggregate;
        
        for (int i=0; i<numKeys; i++) {
            String key = keys.get(i);
            double weight = weights.get(i);
            
            ValueEntry v = Carade.db.get(client.getDbIndex(), key);
            if (v != null && v.type == DataType.ZSET) {
                CaradeZSet zset = (CaradeZSet) v.getValue();
                for (Map.Entry<String, Double> entry : zset.scores.entrySet()) {
                    String member = entry.getKey();
                    double val = entry.getValue() * weight;
                    
                    finalMap.compute(member, (k, oldVal) -> {
                        if (oldVal == null) return val;
                        if (finalAggregate.equals("SUM")) return oldVal + val;
                        if (finalAggregate.equals("MIN")) return Math.min(oldVal, val);
                        if (finalAggregate.equals("MAX")) return Math.max(oldVal, val);
                        return oldVal;
                    });
                }
            }
        }
        
        // Sort result
        CaradeZSet resultZSet = new CaradeZSet();
        for (Map.Entry<String, Double> entry : finalMap.entrySet()) {
            resultZSet.add(entry.getValue(), entry.getKey());
        }
        
        List<byte[]> result = new ArrayList<>();
        for (ZNode node : resultZSet.sorted) {
            result.add(node.member.getBytes(StandardCharsets.UTF_8));
            if (withScores) {
                result.add(String.valueOf(node.score).getBytes(StandardCharsets.UTF_8));
            }
        }
        
        client.sendArray(result);
    }
}
