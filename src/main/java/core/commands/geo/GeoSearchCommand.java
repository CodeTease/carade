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
import java.nio.charset.StandardCharsets;

public class GeoSearchCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        // GEOSEARCH key [FROMMEMBER member | FROMLONLAT lon lat] [BYRADIUS radius unit | BYBOX width height unit] [ASC|DESC] [COUNT count] [WITHCOORD] [WITHDIST] [WITHHASH]
        if (args.size() < 4) {
            client.sendError("ERR wrong number of arguments for 'geosearch' command");
            return;
        }

        try {
            String key = new String(args.get(1), StandardCharsets.UTF_8);
            CaradeDatabase db = CaradeDatabase.getInstance();
            ValueEntry entry = db.get(client.getDbIndex(), key);
            
            if (entry == null || entry.type != DataType.ZSET) {
                if (entry == null) client.sendArray(new ArrayList<>());
                else client.sendError("WRONGTYPE");
                return;
            }

            CaradeZSet zset = (CaradeZSet) entry.getValue();
            
            double originLat = 0, originLon = 0;
            int argIdx = 2;
            
            // 1. Parse Origin
            String originType = new String(args.get(argIdx++), StandardCharsets.UTF_8).toUpperCase();
            if (originType.equals("FROMMEMBER")) {
                if (argIdx >= args.size()) { client.sendError("ERR syntax error"); return; }
                String member = new String(args.get(argIdx++), StandardCharsets.UTF_8);
                Double score = zset.score(member);
                if (score == null) {
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
            boolean withCoord = false;
            boolean withDist = false;
            boolean withHash = false;
            int count = Integer.MAX_VALUE;
            
            while (argIdx < args.size()) {
                String opt = new String(args.get(argIdx++), StandardCharsets.UTF_8).toUpperCase();
                if (opt.equals("ASC")) sortAsc = true;
                else if (opt.equals("DESC")) sortDesc = true;
                else if (opt.equals("WITHCOORD")) withCoord = true;
                else if (opt.equals("WITHDIST")) withDist = true;
                else if (opt.equals("WITHHASH")) withHash = true;
                else if (opt.equals("COUNT")) {
                    if (argIdx < args.size()) count = Integer.parseInt(new String(args.get(argIdx++), StandardCharsets.UTF_8));
                }
            }
            
            // Search
            List<GeoResult> results = new ArrayList<>();
            for (Map.Entry<String, Double> e : zset.scores.entrySet()) {
                String member = e.getKey();
                Double score = e.getValue();
                double[] coords = GeoUtils.decode(score.longValue());
                
                if (radiusMeters > 0) {
                    double dist = GeoUtils.distance(originLat, originLon, coords[0], coords[1]);
                    if (dist <= radiusMeters) {
                        results.add(new GeoResult(member, dist, score.longValue(), coords));
                    }
                } else {
                    // Box check: rough approximation using latitude/longitude diffs converted to meters
                    // 1 deg lat ~= 111km. 1 deg lon ~= 111km * cos(lat)
                    double dLat = Math.abs(coords[0] - originLat);
                    double dLon = Math.abs(coords[1] - originLon);
                    
                    double latMeters = dLat * 111139; // approx meters per degree lat
                    double lonMeters = dLon * 111139 * Math.cos(Math.toRadians(originLat));
                    
                    if (latMeters <= heightMeters/2 && lonMeters <= widthMeters/2) {
                        double dist = GeoUtils.distance(originLat, originLon, coords[0], coords[1]);
                         results.add(new GeoResult(member, dist, score.longValue(), coords));
                    }
                }
            }
            
            // Sort
            if (sortAsc) {
                results.sort(Comparator.comparingDouble(r -> r.dist));
            } else if (sortDesc) {
                results.sort((r1, r2) -> Double.compare(r2.dist, r1.dist));
            }
            
            // Limit
            if (results.size() > count) {
                results = results.subList(0, count);
            }
            
            // Response
            List<Object> response = new ArrayList<>();
            for (GeoResult r : results) {
                if (!withCoord && !withDist && !withHash) {
                    response.add(r.member.getBytes(StandardCharsets.UTF_8));
                } else {
                    List<Object> item = new ArrayList<>();
                    item.add(r.member.getBytes(StandardCharsets.UTF_8));
                    if (withDist) item.add(String.format("%.4f", r.dist).getBytes(StandardCharsets.UTF_8)); 
                    if (withHash) item.add(r.hash);
                    if (withCoord) {
                        List<byte[]> c = new ArrayList<>();
                        c.add(String.format("%.6f", r.coords[1]).getBytes(StandardCharsets.UTF_8));
                        c.add(String.format("%.6f", r.coords[0]).getBytes(StandardCharsets.UTF_8));
                        item.add(c);
                    }
                    response.add(item);
                }
            }
            client.sendMixedArray(response);
            
        } catch (Exception e) {
            client.sendError("ERR " + e.getMessage());
        }
    }
    
    public static class GeoResult {
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
