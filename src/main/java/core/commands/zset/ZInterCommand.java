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

public class ZInterCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("usage: ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]");
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

        // Inter Logic
        // Find smallest set to iterate?
        // Or just map intersection.
        
        Map<String, Double> finalMap = new HashMap<>();
        boolean first = true;
        
        for (int i=0; i<numKeys; i++) {
            String key = keys.get(i);
            double weight = weights.get(i);
            
            ValueEntry v = Carade.db.get(client.getDbIndex(), key);
            if (v == null || v.type != DataType.ZSET) {
                // If any key is missing or not zset, intersection is empty (unless we treat missing as empty set)
                // Redis: ZINTER with missing key -> empty set.
                client.sendArray(new ArrayList<>());
                return;
            }
            
            CaradeZSet zset = (CaradeZSet) v.getValue();
            if (zset.scores.isEmpty()) {
                client.sendArray(new ArrayList<>());
                return;
            }
            
            if (first) {
                for (Map.Entry<String, Double> entry : zset.scores.entrySet()) {
                    finalMap.put(entry.getKey(), entry.getValue() * weight);
                }
                first = false;
            } else {
                Map<String, Double> nextMap = new HashMap<>();
                for (Map.Entry<String, Double> entry : zset.scores.entrySet()) {
                    String member = entry.getKey();
                    if (finalMap.containsKey(member)) {
                        double oldScore = finalMap.get(member);
                        double newScore = entry.getValue() * weight;
                        
                        if (aggregate.equals("SUM")) {
                            nextMap.put(member, oldScore + newScore);
                        } else if (aggregate.equals("MIN")) {
                            nextMap.put(member, Math.min(oldScore, newScore));
                        } else if (aggregate.equals("MAX")) {
                            nextMap.put(member, Math.max(oldScore, newScore));
                        }
                    }
                }
                finalMap = nextMap;
                if (finalMap.isEmpty()) break;
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
