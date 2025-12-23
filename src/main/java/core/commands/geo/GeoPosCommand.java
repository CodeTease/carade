package core.commands.geo;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import core.utils.GeoUtils;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GeoPosCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: GEOPOS key [member ...]");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        
        List<Object> response = new ArrayList<>(); // List of Lists or Nulls
        int count = args.size() - 2;
        
        if (entry == null) {
             for (int i = 0; i < count; i++) {
                response.add(null);
            }
        } else if (entry.type != DataType.ZSET) {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        } else {
            CaradeZSet zset = (CaradeZSet) entry.getValue();
            for (int i = 2; i < args.size(); i++) {
                String member = new String(args.get(i), StandardCharsets.UTF_8);
                Double score = zset.score(member);
                if (score == null) {
                    response.add(null);
                } else {
                    double[] latlon = GeoUtils.decode(score.longValue());
                    List<byte[]> coord = new ArrayList<>();
                    coord.add(String.valueOf(latlon[1]).getBytes(StandardCharsets.UTF_8)); // Longitude first
                    coord.add(String.valueOf(latlon[0]).getBytes(StandardCharsets.UTF_8)); // Latitude second
                    response.add(coord);
                }
            }
        }
        
        client.sendMixedArray(response);
    }
}
