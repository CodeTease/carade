package core.commands.geo;

import core.commands.Command;
import core.network.ClientHandler;
import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.db.DataType;
import core.structs.CaradeZSet;
import core.utils.GeoUtils;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

public class GeoRadiusCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
        // args[0] = GEORADIUS
        // args[1] = key
        // args[2] = lon
        // args[3] = lat
        // args[4] = radius
        // args[5] = unit
        // Options start from args[6]
        
        if (args.size() < 6) {
            client.sendError("ERR wrong number of arguments for 'georadius' command");
            return;
        }

        try {
            String key = new String(args.get(1));
            double lon = Double.parseDouble(new String(args.get(2)));
            double lat = Double.parseDouble(new String(args.get(3)));
            double radius = Double.parseDouble(new String(args.get(4)));
            String unit = new String(args.get(5));

            boolean withCoord = false;
            boolean withDist = false;
            boolean withHash = false; 
            boolean sortAsc = false; 
            boolean sortDesc = false;
            
            // Parse options
            for (int i = 6; i < args.size(); i++) {
                String arg = new String(args.get(i)).toUpperCase();
                if (arg.equals("WITHCOORD")) withCoord = true;
                else if (arg.equals("WITHDIST")) withDist = true;
                else if (arg.equals("WITHHASH")) withHash = true;
                else if (arg.equals("ASC")) sortAsc = true;
                else if (arg.equals("DESC")) sortDesc = true;
            }
            
            CaradeDatabase db = CaradeDatabase.getInstance();
            ValueEntry entry = db.get(key);
            if (entry == null || entry.type != DataType.ZSET) {
                client.sendArray(new ArrayList<>());
                return;
            }

            CaradeZSet zset = (CaradeZSet) entry.value;
            double radiusMeters = GeoUtils.convertToMeters(radius, unit);
            
            List<GeoResult> results = new ArrayList<>();

            // Naive implementation
            for (Map.Entry<String, Double> e : zset.scores.entrySet()) {
                String member = e.getKey();
                Double score = e.getValue();
                
                double[] coords = GeoUtils.decode(score.longValue());
                double dist = GeoUtils.distance(lat, lon, coords[0], coords[1]);
                
                if (dist <= radiusMeters) {
                    results.add(new GeoResult(member, dist, score.longValue(), coords));
                }
            }
            
            // Sort
            if (sortAsc) {
                results.sort(Comparator.comparingDouble(r -> r.dist));
            } else if (sortDesc) {
                results.sort((r1, r2) -> Double.compare(r2.dist, r1.dist));
            }

            List<Object> response = new ArrayList<>();
            for (GeoResult r : results) {
                if (!withCoord && !withDist && !withHash) {
                    response.add(r.member.getBytes());
                } else {
                    List<Object> item = new ArrayList<>();
                    item.add(r.member.getBytes());
                    if (withDist) {
                        item.add(String.format("%.4f", GeoUtils.convertDistance(r.dist, unit)).getBytes());
                    }
                    if (withHash) {
                        item.add(r.hash);
                    }
                    if (withCoord) {
                         List<byte[]> coords = new ArrayList<>();
                         coords.add(String.format("%.6f", r.coords[1]).getBytes()); // Lon
                         coords.add(String.format("%.6f", r.coords[0]).getBytes()); // Lat
                         item.add(coords);
                    }
                    response.add(item);
                }
            }
            
            client.sendMixedArray(response);

        } catch (NumberFormatException e) {
            client.sendError("ERR value is not a valid float");
        } catch (Exception e) {
            client.sendError("ERR " + e.getMessage());
        }
    }
    
    private static class GeoResult {
        String member;
        double dist;
        long hash;
        double[] coords;
        
        public GeoResult(String member, double dist, long hash, double[] coords) {
            this.member = member;
            this.dist = dist;
            this.hash = hash;
            this.coords = coords;
        }
    }
}
