package core.commands.set;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SScanCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 3) {
            client.sendError("ERR wrong number of arguments for 'sscan' command");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        String cursor = new String(args.get(2), StandardCharsets.UTF_8);
        String pattern = null;
        int count = 10;

        for (int i = 3; i < args.size(); i++) {
            String arg = new String(args.get(i), StandardCharsets.UTF_8).toUpperCase();
            if (arg.equals("MATCH") && i + 1 < args.size()) {
                pattern = new String(args.get(++i), StandardCharsets.UTF_8);
            } else if (arg.equals("COUNT") && i + 1 < args.size()) {
                try {
                    count = Integer.parseInt(new String(args.get(++i), StandardCharsets.UTF_8));
                } catch (Exception e) {}
            }
        }

        java.util.regex.Pattern regex = null;
        if (pattern != null) {
            String r = pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".");
            regex = java.util.regex.Pattern.compile(r);
        }

        Iterator<?> it;
        Carade.ScanCursor sc = null;
        int dbIndex = client.getDbIndex();

        if (cursor.equals("0")) {
            ValueEntry entry = Carade.db.get(dbIndex, key);
            if (entry == null || entry.type != DataType.SET) {
                client.send(client.isResp(), Resp.array(Arrays.asList("0".getBytes(StandardCharsets.UTF_8), Resp.array(Collections.emptyList()))), null);
                return;
            }
            it = ((Set<String>) entry.getValue()).iterator();
            sc = new Carade.ScanCursor(it, null);
            String newCursor = String.valueOf(Carade.cursorIdGen.getAndIncrement());
            Carade.scanRegistry.put(newCursor, sc);
            cursor = newCursor;
        } else {
            sc = Carade.scanRegistry.get(cursor);
            if (sc == null) {
                cursor = "0";
                it = Collections.emptyIterator();
            } else {
                it = sc.iterator;
                sc.lastAccess = System.currentTimeMillis();
            }
        }

        List<byte[]> results = new ArrayList<>();
        int found = 0;
        while (it.hasNext() && found < count) {
            String k = (String) it.next();
            found++;
            if (regex == null || regex.matcher(k).matches()) {
                results.add(k.getBytes(StandardCharsets.UTF_8));
            }
        }

        if (!it.hasNext()) {
            Carade.scanRegistry.remove(cursor);
            cursor = "0";
        }

        if (client.isResp()) {
            List<byte[]> outer = new ArrayList<>();
            outer.add(cursor.getBytes(StandardCharsets.UTF_8));
            outer.add(Resp.array(results));
            client.send(true, Resp.array(outer), null);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("1) \"").append(cursor).append("\"\n");
            sb.append("2) ");
            for (int i = 0; i < results.size(); i++) {
                sb.append(i == 0 ? "" : "\n   ").append(i + 1).append(") \"").append(new String(results.get(i), StandardCharsets.UTF_8)).append("\"");
            }
            client.send(false, null, sb.toString());
        }
    }
}
