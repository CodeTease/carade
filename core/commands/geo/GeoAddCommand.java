package core.commands.geo;

import core.commands.Command;
import core.network.ClientHandler;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.db.DataType;
import core.structs.CaradeZSet;
import core.utils.GeoUtils;
import java.util.List;

public class GeoAddCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // GEOADD key lon lat member [lon lat member ...]
        // args[0] is GEOADD
        // args[1] is key
        // args[2] is lon
        // args[3] is lat
        // args[4] is member
        // Total size must be at least 5 (1+1+3)
        // (size - 2) must be divisible by 3.
        
        if (args.size() < 5 || (args.size() - 2) % 3 != 0) {
            client.sendError("ERR wrong number of arguments for 'geoadd' command");
            return;
        }

        String key = new String(args.get(1));
        CaradeDatabase db = CaradeDatabase.getInstance();
        ValueEntry entry = db.get(key);
        CaradeZSet zset;

        if (entry == null) {
            zset = new CaradeZSet();
            db.put(key, new ValueEntry(zset, DataType.ZSET, -1));
        } else if (entry.type != DataType.ZSET) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        } else {
            zset = (CaradeZSet) entry.value;
        }

        int added = 0;
        int argIndex = 2;
        while (argIndex < args.size()) {
            try {
                double lon = Double.parseDouble(new String(args.get(argIndex++)));
                double lat = Double.parseDouble(new String(args.get(argIndex++)));
                String member = new String(args.get(argIndex++));

                if (lat < GeoUtils.LAT_MIN || lat > GeoUtils.LAT_MAX || 
                    lon < GeoUtils.LON_MIN || lon > GeoUtils.LON_MAX) {
                     client.sendError("ERR invalid longitude,latitude pair " + lon + "," + lat);
                     return; 
                }

                long hash = GeoUtils.encode(lat, lon);
                added += zset.add((double) hash, member);
            } catch (NumberFormatException e) {
                client.sendError("ERR value is not a valid float");
                return;
            } catch (IllegalArgumentException e) {
                client.sendError("ERR " + e.getMessage());
                return;
            }
        }

        client.sendInteger(added);
    }
}
