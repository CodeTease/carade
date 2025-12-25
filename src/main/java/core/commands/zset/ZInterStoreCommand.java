package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ZInterStoreCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]");
            return;
        }
        
        try {
            String dest = new String(args.get(1), StandardCharsets.UTF_8);
            int numKeys = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
            if (numKeys < 1) {
                client.sendError("ERR at least 1 input key is needed for ZINTERSTORE");
                return;
            }
            
            List<String> srcKeys = new ArrayList<>();
            for(int i=0; i<numKeys; i++) {
                if (3+i >= args.size()) throw new IndexOutOfBoundsException();
                srcKeys.add(new String(args.get(3+i), StandardCharsets.UTF_8));
            }
            
            int nextArg = 3 + numKeys;
            List<Double> weights = new ArrayList<>();
            String aggregate = "SUM";
            
            while(nextArg < args.size()) {
                String arg = new String(args.get(nextArg++), StandardCharsets.UTF_8).toUpperCase();
                if (arg.equals("WEIGHTS")) {
                    for(int i=0; i<numKeys; i++) {
                        if (nextArg >= args.size()) throw new IllegalArgumentException("ERR syntax error");
                        weights.add(Double.parseDouble(new String(args.get(nextArg++), StandardCharsets.UTF_8)));
                    }
                } else if (arg.equals("AGGREGATE")) {
                    if (nextArg >= args.size()) throw new IllegalArgumentException("ERR syntax error");
                    aggregate = new String(args.get(nextArg++), StandardCharsets.UTF_8).toUpperCase();
                }
            }
            if (weights.isEmpty()) for(int i=0; i<numKeys; i++) weights.add(1.0);
            
            final String aggOp = aggregate;
            final int[] sizeRef = {0};
            
            // Serialize args roughly
            Object[] cmdArgs = new Object[args.size()-1];
            for(int i=1; i<args.size(); i++) cmdArgs[i-1] = new String(args.get(i), StandardCharsets.UTF_8);

            final List<Double> finalWeights = weights;
            
            client.executeWrite(() -> {
                Map<String, Double> finalScores = new HashMap<>();
                
                // Initialize with first set
                ValueEntry e = Carade.db.get(client.getDbIndex(), srcKeys.get(0));
                if (e != null && e.type == DataType.ZSET) {
                    CaradeZSet zs = (CaradeZSet) e.getValue();
                    for(Map.Entry<String, Double> entry : zs.scores.entrySet()) {
                        finalScores.put(entry.getKey(), entry.getValue() * finalWeights.get(0));
                    }
                }
                
                // ZINTERSTORE intersection logic
                for(int i=1; i<numKeys; i++) {
                    String k = srcKeys.get(i);
                    double w = finalWeights.get(i);
                    e = Carade.db.get(client.getDbIndex(), k);
                    if (e == null || e.type != DataType.ZSET) {
                        finalScores.clear();
                        break;
                    }
                    CaradeZSet zs = (CaradeZSet) e.getValue();
                    Iterator<Map.Entry<String, Double>> it = finalScores.entrySet().iterator();
                    while(it.hasNext()) {
                        Map.Entry<String, Double> ent = it.next();
                        Double s = zs.score(ent.getKey());
                        if (s == null) {
                            it.remove();
                        } else {
                            double newScore = s * w;
                            if (aggOp.equals("MIN")) ent.setValue(Math.min(ent.getValue(), newScore));
                            else if (aggOp.equals("MAX")) ent.setValue(Math.max(ent.getValue(), newScore));
                            else ent.setValue(ent.getValue() + newScore);
                        }
                    }
                }
                
                if (finalScores.isEmpty()) {
                    Carade.db.remove(client.getDbIndex(), dest);
                } else {
                    CaradeZSet resZ = new CaradeZSet();
                    for(Map.Entry<String, Double> ent : finalScores.entrySet()) {
                        resZ.add(ent.getValue(), ent.getKey());
                    }
                    Carade.db.put(client.getDbIndex(), dest, new ValueEntry(resZ, DataType.ZSET, -1));
                }
                sizeRef[0] = finalScores.size();
                Carade.notifyWatchers(dest);
            }, "ZINTERSTORE", cmdArgs);
            
            client.sendInteger(sizeRef[0]);
            
        } catch (Exception e) {
            client.sendError("ERR syntax error");
        }
    }
}
