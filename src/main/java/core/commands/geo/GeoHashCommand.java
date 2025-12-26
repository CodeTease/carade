package core.commands.geo;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.structs.CaradeZSet;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GeoHashCommand implements Command {
    private static final char[] BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz".toCharArray();

    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: GEOHASH key [member ...]");
            return;
        }
        
        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.getDbIndex(), key);
        
        List<byte[]> response = new ArrayList<>();
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
                    response.add(toGeoHash(score.longValue()).getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        client.sendArray(response);
    }
    
    private String toGeoHash(long hash) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 11; i++) {
            int shift = 52 - (i + 1) * 5; 
            int val = 0;
            if (shift >= 0) {
                val = (int) ((hash >> shift) & 0x1F);
            } else {
                // Last chunk: bits 1, 0. 
                // Left align them in the 5-bit window: |b1|b0|0|0|0|
                val = (int) ((hash & 0x03) << 3);
            }
            sb.append(BASE32[val]);
        }
        return sb.toString();
    }
}
