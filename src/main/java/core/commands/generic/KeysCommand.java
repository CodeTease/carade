package core.commands.generic;

import core.Carade;
import core.commands.Command;
import core.network.ClientHandler;
import core.protocol.Resp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class KeysCommand implements Command {
    @Override
    public void execute(ClientHandler client, List<byte[]> args) {
        if (args.size() < 2) {
            client.sendError("usage: KEYS pattern");
            return;
        }
        String pattern = new String(args.get(1), StandardCharsets.UTF_8);
        List<byte[]> keys = new ArrayList<>();
        List<String> keyStrings = new ArrayList<>();
        int dbIndex = client.getDbIndex();
        
        if (pattern.equals("*")) {
            for(String k : Carade.db.keySet(dbIndex)) {
                keys.add(k.getBytes(StandardCharsets.UTF_8));
                keyStrings.add(k);
            }
        } else {
            String regex = pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".");
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
            for (String k : Carade.db.keySet(dbIndex)) {
                if (p.matcher(k).matches()) {
                    keys.add(k.getBytes(StandardCharsets.UTF_8));
                    keyStrings.add(k);
                }
            }
        }
        
        if (client.isResp()) {
            client.send(true, Resp.array(keys), null);
        } else {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < keyStrings.size(); i++) {
                sb.append((i+1) + ") \"" + keyStrings.get(i) + "\"\n");
            }
            client.send(false, null, sb.toString().trim());
        }
    }
}
