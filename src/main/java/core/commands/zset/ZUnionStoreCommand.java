package core.commands.zset;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ZUnionStoreCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 4) {
            client.sendError("usage: ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]");
            return;
        }
        
        try {
            String dest = new String(args.get(1), StandardCharsets.UTF_8);
            int numKeys = Integer.parseInt(new String(args.get(2), StandardCharsets.UTF_8));
            if (numKeys < 1) {
                client.sendError("ERR at least 1 input key is needed for ZUNIONSTORE");
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
                
                for(int i=0; i<numKeys; i++) {
                    String k = srcKeys.get(i);
                    double w = finalWeights.get(i);
                    ValueEntry e = Carade.db.get(client.getDbIndex(), k);
                    if (e != null && e.type == DataType.ZSET) {
                        CaradeZSet zs = (CaradeZSet) e.getValue();
                        for(Map.Entry<String, Double> entry : zs.scores.entrySet()) {
                            String member = entry.getKey();
                            double score = entry.getValue() * w;
                            finalScores.merge(member, score, (oldV, newV) -> {
                                if (aggOp.equals("MIN")) return Math.min(oldV, newV);
                                if (aggOp.equals("MAX")) return Math.max(oldV, newV);
                                return oldV + newV;
                            });
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
            }, "ZUNIONSTORE", cmdArgs);
            
            client.sendInteger(sizeRef[0]);
            
        } catch (Exception e) {
            client.sendError("ERR syntax error");
        }
    }
}
