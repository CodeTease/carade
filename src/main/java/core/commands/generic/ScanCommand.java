package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import core.structs.CaradeZSet;
import core.structs.CaradeHash;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ScanCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
         String cmd = new String(args.get(0), StandardCharsets.UTF_8).toUpperCase();
         int cursorIdx = cmd.equals("SCAN") ? 1 : 2;
         if (args.size() <= cursorIdx) {
             client.sendError("wrong number of arguments for '" + cmd.toLowerCase() + "' command");
             return;
         }
         
         String key = null;
         if (!cmd.equals("SCAN")) {
             key = new String(args.get(1), StandardCharsets.UTF_8);
         }

         String cursor = new String(args.get(cursorIdx), StandardCharsets.UTF_8);
         String pattern = null;
         String typeFilter = null;
         int count = 10;
         
         for (int i = cursorIdx + 1; i < args.size(); i++) {
             String arg = new String(args.get(i), StandardCharsets.UTF_8).toUpperCase();
             if (arg.equals("MATCH") && i + 1 < args.size()) {
                 pattern = new String(args.get(++i), StandardCharsets.UTF_8);
             } else if (arg.equals("COUNT") && i + 1 < args.size()) {
                 try { count = Integer.parseInt(new String(args.get(++i), StandardCharsets.UTF_8)); } catch (Exception e) {}
             } else if (arg.equals("TYPE") && i + 1 < args.size()) {
                 typeFilter = new String(args.get(++i), StandardCharsets.UTF_8).toUpperCase();
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
             // New iterator
             if (cmd.equals("SCAN")) {
                 it = Carade.db.keySet(dbIndex).iterator();
             } else {
                 ValueEntry entry = Carade.db.get(dbIndex, key);
                 if (entry == null) {
                      client.send(client.isResp(), Resp.array(Arrays.asList("0".getBytes(StandardCharsets.UTF_8), Resp.array(Collections.emptyList()))), null);
                     return;
                 }
                 if (cmd.equals("HSCAN") && entry.type == DataType.HASH) {
                     if (entry.getValue() instanceof CaradeHash) {
                         it = ((CaradeHash)entry.getValue()).map.entrySet().iterator();
                     } else {
                         it = ((ConcurrentHashMap<String, String>)entry.getValue()).entrySet().iterator();
                     }
                 } else if (cmd.equals("SSCAN") && entry.type == DataType.SET) {
                     it = ((Set<String>)entry.getValue()).iterator();
                 } else if (cmd.equals("ZSCAN") && entry.type == DataType.ZSET) {
                     it = ((CaradeZSet)entry.getValue()).scores.entrySet().iterator();
                 } else {
                     client.send(client.isResp(), Resp.array(Arrays.asList("0".getBytes(StandardCharsets.UTF_8), Resp.array(Collections.emptyList()))), null);
                     return;
                 }
             }
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
             Object next = it.next();
             found++; 
             
             String k = null;
             
             if (cmd.equals("SCAN") || cmd.equals("SSCAN")) {
                 k = (String) next;
                 boolean matches = true;
                 if (cmd.equals("SCAN") && typeFilter != null) {
                     ValueEntry e = Carade.db.get(dbIndex, k);
                     if (e == null || !e.type.name().equals(typeFilter)) matches = false;
                 }
                 
                 if (matches && (regex == null || regex.matcher(k).matches())) {
                     results.add(k.getBytes(StandardCharsets.UTF_8));
                 }
             } else if (cmd.equals("HSCAN")) {
                 Map.Entry<String, String> e = (Map.Entry<String, String>) next;
                 if (regex == null || regex.matcher(e.getKey()).matches()) {
                     results.add(e.getKey().getBytes(StandardCharsets.UTF_8));
                     results.add(e.getValue().getBytes(StandardCharsets.UTF_8));
                 }
             } else if (cmd.equals("ZSCAN")) {
                 Map.Entry<String, Double> e = (Map.Entry<String, Double>) next;
                 if (regex == null || regex.matcher(e.getKey()).matches()) {
                     results.add(e.getKey().getBytes(StandardCharsets.UTF_8));
                     String s = String.valueOf(e.getValue());
                     if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                     results.add(s.getBytes(StandardCharsets.UTF_8));
                 }
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
             for (int i=0; i<results.size(); i++) {
                 sb.append(i==0 ? "" : "\n   ").append(i+1).append(") \"").append(new String(results.get(i), StandardCharsets.UTF_8)).append("\"");
             }
             client.send(false, null, sb.toString());
         }
    }
}
