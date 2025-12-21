package core.commands.geo;

import core.commands.Command;
import core.network.ClientHandler;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.db.DataType;
import core.structs.CaradeZSet;
import core.utils.GeoUtils;
import java.util.List;

public class GeoDistCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // GEODIST key member1 member2 [unit]
        // args[0] = GEODIST
        // args[1] = key
        // args[2] = member1
        // args[3] = member2
        // args[4] = unit (optional)
        
        if (args.size() < 4) {
            client.sendError("ERR wrong number of arguments for 'geodist' command");
            return;
        }

        String key = new String(args.get(1));
        String member1 = new String(args.get(2));
        String member2 = new String(args.get(3));
        String unit = args.size() > 4 ? new String(args.get(4)) : "m";

        CaradeDatabase db = CaradeDatabase.getInstance();
        ValueEntry entry = db.get(key);
        if (entry == null || entry.type != DataType.ZSET) {
            client.sendNull();
            return;
        }

        CaradeZSet zset = (CaradeZSet) entry.value;
        Double score1 = zset.score(member1);
        Double score2 = zset.score(member2);

        if (score1 == null || score2 == null) {
            client.sendNull();
            return;
        }

        double[] coord1 = GeoUtils.decode(score1.longValue());
        double[] coord2 = GeoUtils.decode(score2.longValue());

        double distMeters = GeoUtils.distance(coord1[0], coord1[1], coord2[0], coord2[1]);
        double distConverted = GeoUtils.convertDistance(distMeters, unit);

        // Redis returns distance as string (Double string)
        client.sendBulkString(String.format("%.4f", distConverted));
    }
}
