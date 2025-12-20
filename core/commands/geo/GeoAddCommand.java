package core.commands.geo;

import core.commands.Command;
import core.network.ClientHandler;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.db.DataType;
import core.structs.CaradeZSet;
import core.utils.GeoUtils;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class GeoAddCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // GEOADD key lon lat member [lon lat member ...]
        
        if (args.size() < 5 || (args.size() - 2) % 3 != 0) {
            client.sendError("ERR wrong number of arguments for 'geoadd' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        
        // Prepare data before acquiring write lock
        List<Double> scores = new ArrayList<>();
        List<String> members = new ArrayList<>();
        List<String> argStrings = new ArrayList<>(); // For AOF logging

        int argIndex = 2;
        while (argIndex < args.size()) {
            try {
                String lonStr = new String(args.get(argIndex++), StandardCharsets.UTF_8);
                String latStr = new String(args.get(argIndex++), StandardCharsets.UTF_8);
                String member = new String(args.get(argIndex++), StandardCharsets.UTF_8);
                
                double lon = Double.parseDouble(lonStr);
                double lat = Double.parseDouble(latStr);

                if (lat < GeoUtils.LAT_MIN || lat > GeoUtils.LAT_MAX || 
                    lon < GeoUtils.LON_MIN || lon > GeoUtils.LON_MAX) {
                     client.sendError("ERR invalid longitude,latitude pair " + lon + "," + lat);
                     return; 
                }

                long hash = GeoUtils.encode(lat, lon);
                scores.add((double) hash);
                members.add(member);
                
                argStrings.add(lonStr);
                argStrings.add(latStr);
                argStrings.add(member);
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not a valid float");
                return;
            } catch (IllegalArgumentException e) {
                client.sendError("ERR " + e.getMessage());
                return;
            }
        }

        // Execute Write safely
        final int[] addedCount = {0};
        
        Object[] logArgs = new Object[argStrings.size() + 1];
        logArgs[0] = key;
        for(int i=0; i<argStrings.size(); i++) logArgs[i+1] = argStrings.get(i);

        client.executeWrite(() -> {
            CaradeDatabase db = CaradeDatabase.getInstance();
            db.getStore(client.dbIndex).compute(key, (k, v) -> {
                CaradeZSet zset;
                if (v == null) {
                    zset = new CaradeZSet();
                    v = new ValueEntry(zset, DataType.ZSET, -1);
                } else if (v.type != DataType.ZSET) {
                    throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                } else {
                    zset = (CaradeZSet) v.getValue();
                }

                for (int i = 0; i < scores.size(); i++) {
                    addedCount[0] += zset.add(scores.get(i), members.get(i));
                }
                
                v.touch();
                return v;
            });
        }, "GEOADD", logArgs);

        client.sendInteger(addedCount[0]);
    }
}
