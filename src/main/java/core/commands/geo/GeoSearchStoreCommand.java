package core.commands.geo;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.db.ValueEntry;
import core.db.DataType;
import core.structs.CaradeZSet;
import core.utils.GeoUtils;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.nio.charset.StandardCharsets;

public class GeoSearchStoreCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // GEOSEARCHSTORE destination source [FROMMEMBER member | FROMLONLAT lon lat] [BYRADIUS radius unit | BYBOX width height unit] [ASC|DESC] [COUNT count] [STOREDIST]
        if (args.size() < 5) {
            client.sendError("ERR wrong number of arguments for 'geosearchstore' command");
            return;
        }

        try {
            String destination = new String(args.get(1), StandardCharsets.UTF_8);
            String key = new String(args.get(2), StandardCharsets.UTF_8);
            
            ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
            
            if (entry == null || entry.type != DataType.ZSET) {
                if (entry != null) { client.sendError("WRONGTYPE"); return; }
                // Empty source = empty dest
                client.executeWrite(() -> {
                     Carade.db.remove(client.getDbIndex(), destination);
                }, "GEOSEARCHSTORE", destination);
                client.sendInteger(0);
                return;
            }

            CaradeZSet zset = (CaradeZSet) entry.getValue();
            
            double originLat = 0, originLon = 0;
            int argIdx = 3;
            
            // 1. Parse Origin
            String originType = new String(args.get(argIdx++), StandardCharsets.UTF_8).toUpperCase();
            if (originType.equals("FROMMEMBER")) {
                if (argIdx >= args.size()) { client.sendError("ERR syntax error"); return; }
                String member = new String(args.get(argIdx++), StandardCharsets.UTF_8);
                Double score = zset.score(member);
                if (score == null) {
                    // Logic says source member must exist.
                    client.sendError("ERR member not found");
                    return;
                }
                double[] coords = GeoUtils.decode(score.longValue());
                originLat = coords[0];
                originLon = coords[1];
            } else if (originType.equals("FROMLONLAT")) {
                if (argIdx + 1 >= args.size()) { client.sendError("ERR syntax error"); return; }
                originLon = Double.parseDouble(new String(args.get(argIdx++), StandardCharsets.UTF_8));
                originLat = Double.parseDouble(new String(args.get(argIdx++), StandardCharsets.UTF_8));
            } else {
                client.sendError("ERR syntax error");
                return;
            }
            
            // 2. Parse Shape
            if (argIdx >= args.size()) { client.sendError("ERR syntax error"); return; }
            String shapeType = new String(args.get(argIdx++), StandardCharsets.UTF_8).toUpperCase();
            
            double radiusMeters = -1;
            double widthMeters = -1, heightMeters = -1;
            
            if (shapeType.equals("BYRADIUS")) {
                if (argIdx + 1 >= args.size()) { client.sendError("ERR syntax error"); return; }
                double r = Double.parseDouble(new String(args.get(argIdx++), StandardCharsets.UTF_8));
                String u = new String(args.get(argIdx++), StandardCharsets.UTF_8);
                radiusMeters = GeoUtils.convertToMeters(r, u);
            } else if (shapeType.equals("BYBOX")) {
                if (argIdx + 2 >= args.size()) { client.sendError("ERR syntax error"); return; }
                double w = Double.parseDouble(new String(args.get(argIdx++), StandardCharsets.UTF_8));
                double h = Double.parseDouble(new String(args.get(argIdx++), StandardCharsets.UTF_8));
                String u = new String(args.get(argIdx++), StandardCharsets.UTF_8);
                widthMeters = GeoUtils.convertToMeters(w, u);
                heightMeters = GeoUtils.convertToMeters(h, u);
            } else {
                client.sendError("ERR syntax error");
                return;
            }
            
            // 3. Parse Options
            boolean sortAsc = false;
            boolean sortDesc = false;
            boolean storeDist = false;
            int count = Integer.MAX_VALUE;
            
            while (argIdx < args.size()) {
                String opt = new String(args.get(argIdx++), StandardCharsets.UTF_8).toUpperCase();
                if (opt.equals("ASC")) sortAsc = true;
                else if (opt.equals("DESC")) sortDesc = true;
                else if (opt.equals("STOREDIST")) storeDist = true;
                else if (opt.equals("COUNT")) {
                    if (argIdx < args.size()) count = Integer.parseInt(new String(args.get(argIdx++), StandardCharsets.UTF_8));
                }
            }
            
            // Search
            List<GeoSearchCommand.GeoResult> results = new ArrayList<>();
            for (Map.Entry<String, Double> e : zset.scores.entrySet()) {
                String member = e.getKey();
                Double score = e.getValue();
                double[] coords = GeoUtils.decode(score.longValue());
                
                if (radiusMeters > 0) {
                    double dist = GeoUtils.distance(originLat, originLon, coords[0], coords[1]);
                    if (dist <= radiusMeters) {
                        results.add(new GeoSearchCommand.GeoResult(member, dist, score.longValue(), coords));
                    }
                } else {
                    double dLat = Math.abs(coords[0] - originLat);
                    double dLon = Math.abs(coords[1] - originLon);
                    
                    double latMeters = dLat * 111139;
                    double lonMeters = dLon * 111139 * Math.cos(Math.toRadians(originLat));
                    
                    if (latMeters <= heightMeters/2 && lonMeters <= widthMeters/2) {
                        double dist = GeoUtils.distance(originLat, originLon, coords[0], coords[1]);
                         results.add(new GeoSearchCommand.GeoResult(member, dist, score.longValue(), coords));
                    }
                }
            }
            
            if (sortAsc) {
                results.sort(Comparator.comparingDouble(r -> r.dist));
            } else if (sortDesc) {
                results.sort((r1, r2) -> Double.compare(r2.dist, r1.dist));
            }
            
            if (results.size() > count) {
                results = results.subList(0, count);
            }
            
            // Store
            final List<GeoSearchCommand.GeoResult> finalResults = results;
            final boolean finalStoreDist = storeDist;
            
            client.executeWrite(() -> {
                CaradeZSet destZset = new CaradeZSet();
                for (GeoSearchCommand.GeoResult r : finalResults) {
                     // If STOREDIST, score is distance. Else, score is geohash (original score).
                     double score = finalStoreDist ? r.dist : (double)r.hash;
                     destZset.add(score, r.member);
                }
                Carade.db.put(client.getDbIndex(), destination, new ValueEntry(destZset, DataType.ZSET, -1));
            }, "GEOSEARCHSTORE", destination);
            
            client.sendInteger(finalResults.size());
            
        } catch (Exception e) {
            client.sendError("ERR " + e.getMessage());
        }
    }
}
