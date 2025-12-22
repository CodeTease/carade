package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.db.DataType;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.protocol.Resp;
import core.structs.CaradeZSet;
import core.structs.ZNode;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class SortCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]");
            return;
        }

        String key = new String(args.get(1), StandardCharsets.UTF_8);
        ValueEntry entry = Carade.db.get(client.dbIndex, key);

        if (entry == null) {
            client.sendArray(Collections.emptyList());
            return;
        }

        List<String> list = new ArrayList<>();
        if (entry.type == DataType.LIST) {
            list.addAll((Collection<String>) entry.getValue());
        } else if (entry.type == DataType.SET) {
            list.addAll((Collection<String>) entry.getValue());
        } else if (entry.type == DataType.ZSET) {
            CaradeZSet zset = (CaradeZSet) entry.getValue();
            for (ZNode node : zset.sorted) {
                list.add(node.member);
            }
        } else {
            client.sendError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }

        // Parsing options
        String byPattern = null;
        boolean desc = false;
        boolean alpha = false;
        int limitOffset = 0;
        int limitCount = list.size();
        String storeKey = null;
        List<String> getPatterns = new ArrayList<>();

        for (int i = 2; i < args.size(); i++) {
            String arg = new String(args.get(i), StandardCharsets.UTF_8).toUpperCase();
            if (arg.equals("BY") && i + 1 < args.size()) {
                byPattern = new String(args.get(++i), StandardCharsets.UTF_8);
            } else if (arg.equals("LIMIT") && i + 2 < args.size()) {
                limitOffset = Integer.parseInt(new String(args.get(++i), StandardCharsets.UTF_8));
                limitCount = Integer.parseInt(new String(args.get(++i), StandardCharsets.UTF_8));
            } else if (arg.equals("GET") && i + 1 < args.size()) {
                getPatterns.add(new String(args.get(++i), StandardCharsets.UTF_8));
            } else if (arg.equals("ASC")) {
                desc = false;
            } else if (arg.equals("DESC")) {
                desc = true;
            } else if (arg.equals("ALPHA")) {
                alpha = true;
            } else if (arg.equals("STORE") && i + 1 < args.size()) {
                storeKey = new String(args.get(++i), StandardCharsets.UTF_8);
            }
        }

        // Sorting Logic
        final String finalBy = byPattern;
        final boolean finalAlpha = alpha;
        final int dbIdx = client.dbIndex;

        try {
            Collections.sort(list, (a, b) -> {
                double sa = 0, sb = 0;
                String strA = a, strB = b;

                if (finalBy != null) {
                    // Fetch external weight
                    String ka = lookupKey(finalBy, a);
                    String kb = lookupKey(finalBy, b);
                    strA = getValue(dbIdx, ka);
                    strB = getValue(dbIdx, kb);
                    if (strA == null) strA = ""; // Or 0
                    if (strB == null) strB = "";
                }

                if (!finalAlpha) {
                    try {
                        sa = Double.parseDouble(strA);
                        sb = Double.parseDouble(strB);
                        return Double.compare(sa, sb);
                    } catch (Exception e) {
                        // Fallback to alpha if not number? Redis usually errors or treats as 0. 
                        // Treating as 0.
                        return Double.compare(0, 0);
                    }
                } else {
                    return strA.compareTo(strB);
                }
            });
        } catch (Exception e) {
            client.sendError("ERR One or more scores can't be converted into double");
            return;
        }

        if (desc) Collections.reverse(list);

        // Apply Limit
        if (limitOffset < 0) limitOffset = 0;
        if (limitCount < 0) limitCount = 0; // Or max?
        int end = Math.min(limitOffset + limitCount, list.size());
        if (limitOffset >= list.size()) {
            list = Collections.emptyList();
        } else {
            list = list.subList(limitOffset, end);
        }

        // Projection (GET)
        List<String> resultList = new ArrayList<>();
        if (getPatterns.isEmpty()) {
            resultList = list;
        } else {
            for (String elem : list) {
                for (String pattern : getPatterns) {
                    if (pattern.equals("#")) {
                        resultList.add(elem);
                    } else {
                        String k = lookupKey(pattern, elem);
                        String val = getValue(dbIdx, k);
                        resultList.add(val); // Nulls are allowed (nil)
                    }
                }
            }
        }

        // Store or Return
        if (storeKey != null) {
            final List<String> toStore = resultList;
            final String fStoreKey = storeKey;
            
            client.executeWrite(() -> {
                ConcurrentLinkedDeque<String> storedList = new ConcurrentLinkedDeque<>(toStore);
                Carade.db.put(client.dbIndex, fStoreKey, new ValueEntry(storedList, DataType.LIST, -1));
                Carade.notifyWatchers(fStoreKey);
            }, "SORT", key, "STORE", storeKey); // Simplified AOF args for brevity, ideally full args
            
            client.sendInteger(toStore.size());
        } else {
            List<byte[]> respList = new ArrayList<>();
            for (String s : resultList) {
                respList.add(s == null ? null : s.getBytes(StandardCharsets.UTF_8));
            }
            client.sendArray(respList);
        }
    }

    private String lookupKey(String pattern, String value) {
        if (!pattern.contains("*")) return pattern;
        return pattern.replace("*", value);
    }

    private String getValue(int dbIndex, String key) {
        // Support "key" or "key->field" for Hash
        String hashField = null;
        if (key.contains("->")) {
            String[] parts = key.split("->", 2);
            key = parts[0];
            hashField = parts[1];
        }

        ValueEntry entry = Carade.db.get(dbIndex, key);
        if (entry == null) return null;

        if (hashField != null) {
            if (entry.type != DataType.HASH) return null;
            ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) entry.getValue();
            return map.get(hashField);
        } else {
            if (entry.type != DataType.STRING) return null;
            return new String((byte[]) entry.getValue(), StandardCharsets.UTF_8);
        }
    }
}
